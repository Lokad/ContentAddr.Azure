using Lokad.Logging;
using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Lokad.ContentAddr.Azure
{

    public interface IDualBlobLogger : ITrace
    {
        [Error("Blob {hash} in account {account} was not in new storage")]
        void InexistantBlob(string account, string hash);

        [Error("Copy for blob {hash} in account {account} failed")]
        void FailedCopy(string account, string hash);

        [Error("Blob {hash} in account {account} cannot be copied to the new storage")]
        void InvalidBlob(string account, string hash);

    }

    /// <summary> The <see cref="ContentAddr.IAzureReadBlobRef"/> for an azure dual persistent store. </summary>
    /// <see cref="DualAzureReadOnlyStore"/>
    /// <remarks>
    ///     Exposes the Azure Storage blob itself either in <see cref="OldBlob"/> (a blob on the old storage version) or 
    ///     in <see cref="NewBlob"/> (a blob on the new storage version). 
    ///     Reading is performed on NewBlob if it exists, 
    ///     otherwise it is performed on OldBlob and a copy from Oldblob to NewBlob is automatically requested.
    ///     
    /// </remarks>
    public sealed class DualAzureBlobRef : IAzureReadBlobRef
    {

        private static readonly IDualBlobLogger Log = Tracer.Bind(() => Log);

        public DualAzureBlobRef(
            string realm,
            Hash hash,
            CloudBlockBlob oldBlob,
            CloudBlockBlob newBlob)
        {
            Hash = hash;
            Realm = realm;
            OldBlob = oldBlob;
            NewBlob = newBlob;
        }

        /// <see cref="IReadBlobRef.Hash"/>
        public Hash Hash { get; }

        /// <summary> The blob name prefix. </summary>
        /// <remarks>
        ///     Used to avoid accidentally mixing blobs from separate customers
        ///     by using a separate prefix (the "realm") for each customer.
        /// </remarks>
        public string Realm { get; }

        /// <summary> The Azure Storage blob where the blob data might be stored if it exists on the old version of the storage. </summary>
        public CloudBlockBlob OldBlob { get; }

        /// <summary> The Azure Storage blob where the blob data might be stored if it exists on the new version of the storage. </summary>
        public CloudBlockBlob NewBlob { get; }

        /// <summary> The actual Storage blob where reading is performed : NewBlob if it exists, Oldblob otherwise
        /// </summary>
        private AzureBlobRef _chosen;

        public async Task<CloudBlockBlob> GetBlob()
        {
            var chosen = await Chosen(CancellationToken.None).ConfigureAwait(false);
            return chosen.Blob;
        }

        /// <summary>
        /// Chose the blob where readind is performed, start copy if the NewBlob does not exist.
        /// </summary>
        /// <param name="cancel"></param>
        /// <returns></returns>
        private async Task<AzureBlobRef> Chosen(CancellationToken cancel)
        {
            if (_chosen != null) return _chosen;
            _chosen = new AzureBlobRef(Realm, Hash, NewBlob);
            if (await AzureRetry.Do(NewBlob.ExistsAsync, cancel).ConfigureAwait(false))
            {
                await AzureRetry.Do(
                    c => NewBlob.FetchAttributesAsync(null, null, null, c),
                    cancel).ConfigureAwait(false);

                switch (NewBlob.CopyState.Status)
                {
                    case CopyStatus.Aborted:
                    case CopyStatus.Failed:
                        if (await AzureRetry.Do(OldBlob.ExistsAsync, cancel).ConfigureAwait(false))
                        {
                            Log.FailedCopy(Realm, NewBlob.Name);
                            _ = StartCopy(cancel);
                            _chosen = new AzureBlobRef(Realm, Hash, OldBlob);
                        }
                        break;
                    case CopyStatus.Invalid:
                        Log.InvalidBlob(Realm, NewBlob.Name);
                        throw new InvalidOperationException(
                            $"Copy for '{NewBlob.Name}' failed ({NewBlob.CopyState.Status})");
                    case CopyStatus.Pending:
                        if (await AzureRetry.Do(OldBlob.ExistsAsync, cancel).ConfigureAwait(false))
                        {
                            _chosen = new AzureBlobRef(Realm, Hash, OldBlob);
                        }
                        break;
                    case CopyStatus.Success:
                        break;
                    default:
                        throw new ArgumentOutOfRangeException();
                }
            }
            else
            {
                if (await AzureRetry.Do(OldBlob.ExistsAsync, cancel).ConfigureAwait(false))
                {
                    Log.InexistantBlob(Realm, NewBlob.Name);
                    _ = StartCopy(cancel);
                    _chosen = new AzureBlobRef(Realm, Hash, OldBlob);
                }
            }

            return _chosen;
        }

        public async Task StartCopy(CancellationToken cancel)
        {
            var oldBlobRef = new AzureBlobRef(Realm, Hash, OldBlob);
            if (!oldBlobRef.Blob.ServiceClient.Credentials.IsSAS)
            {
                // If possible, generate a download URL and use that as the copy
                // source.
                var oldUri = await oldBlobRef.GetDownloadUrlAsync(
                    TimeSpan.FromDays(1),
                    "",
                    "",
                    cancel).ConfigureAwait(false);

                await AzureRetry.Do(c => NewBlob.StartCopyAsync(oldUri, c), cancel);
            }
            else
            {
                // SAS-based access (such as read-only stores) cannot be used to 
                // generate an URL, so we instead use the copy-from-blob primitive, 
                // which is mostly equivalent (but can cause internal errors on the Azure
                // side). 
                await AzureRetry.Do(c => NewBlob.StartCopyAsync(oldBlobRef.Blob, c), cancel);
            }
        }

        /// <see cref="IReadBlobRef.ExistsAsync"/>
        public async Task<bool> ExistsAsync(CancellationToken cancel)
        {
            var chosen = await Chosen(cancel).ConfigureAwait(false);
            return await chosen.ExistsAsync(cancel).ConfigureAwait(false);
        }


        /// <see cref="IReadBlobRef.GetSizeAsync"/>
        public async Task<long> GetSizeAsync(CancellationToken cancel)
        {
            var chosen = await Chosen(cancel).ConfigureAwait(false);
            return await chosen.GetSizeAsync(cancel).ConfigureAwait(false);
        }

        /// <see cref="IReadBlobRef.OpenAsync"/>
        public async Task<Stream> OpenAsync(CancellationToken cancel)
        {
            var chosen = await Chosen(cancel).ConfigureAwait(false);
            return await chosen.OpenAsync(cancel).ConfigureAwait(false);
        }

        /// <see cref="IAzureReadBlobRef.GetDownloadUrlAsync"/> 
        public async Task<Uri> GetDownloadUrlAsync(
            DateTime now,
            TimeSpan life,
            string filename,
            string contentType,
            CancellationToken cancel)
        {
            var chosen = await Chosen(cancel).ConfigureAwait(false);
            return await chosen.GetDownloadUrlAsync(
                now,
                life,
                filename,
                contentType,
                cancel).ConfigureAwait(false);
        }
    }
}
