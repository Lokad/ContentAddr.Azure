using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

namespace Lokad.ContentAddr.Azure
{
    /// <summary>
    ///     Uploads data to a temporary Azure Blob, 
    ///     then copies it over to the permanent content-addressed blob.
    /// </summary>
    public sealed class AzureWriter : AzurePreWriter
    {
        /// <summary>The persistent container where the blob will be written. </summary>
        private readonly CloudBlobContainer _persistent;

        /// <summary> Will be called when comitting. </summary>
        private readonly OnCommit _onCommit;

        /// <summary> Measures how long it took to write the file. </summary>
        private readonly Stopwatch _stopwatch;

        /// <summary> The realm of the store. </summary>
        private readonly string _realm;

        public AzureWriter(
            string realm,
            CloudBlobContainer persistent,
            CloudBlockBlob temporary,
            OnCommit onCommit) : base(temporary)
        {
            _realm = realm;
            _persistent = persistent;
            _onCommit = onCommit;
            _stopwatch = Stopwatch.StartNew();
        }

        /// <see cref="StoreWriter.DoCommitAsync"/>
        protected override Task DoCommitAsync(Hash hash, CancellationToken cancel) =>
            DoOptCommitAsync(hash, null, cancel);

        /// <see cref="StoreWriter.DoOptCommitAsync"/>
        protected override async Task DoOptCommitAsync(Hash hash, Func<Task> optionalWrite, CancellationToken cancel)
        {
            var finalBlob = _persistent.GetBlockBlobReference(AzureReadOnlyStore.AzureBlobName(_realm, hash));

            // Final blob already exists (maybe it was uploaded earlier), do nothing.
            if (await AzureRetry.OrFalse(() => finalBlob.ExistsAsync(null, null, cancel)).ConfigureAwait(false))
            {
                _onCommit?.Invoke(_stopwatch.Elapsed, _realm, hash, finalBlob.Properties.Length, true);
                return;
            }

            if (optionalWrite != null) await optionalWrite().ConfigureAwait(false);
            await WriteTemporary(cancel).ConfigureAwait(false);

            try
            {
                await AzureStore.CopyToPersistent(Temporary, finalBlob, cancel).ConfigureAwait(false);
            }
            finally
            {
                // Always delete the blob if it was created.
                AzureStore.DeleteBlob(Temporary, TimeSpan.FromSeconds(1));
            }

            _onCommit?.Invoke(_stopwatch.Elapsed, _realm, hash, finalBlob.Properties.Length, false);
        }

        /// <summary> A delegate used for logging commit information. </summary>
        public delegate void OnCommit(TimeSpan elapsed, string realm, Hash hash, long size, bool alreadyExists);
    }
}