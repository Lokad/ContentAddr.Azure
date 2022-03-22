using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage;
using System;
using System.Diagnostics;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using System.IO.Compression;
using System.Collections.Generic;

namespace Lokad.ContentAddr.Azure
{
    public enum UnArchiveStatus
    {
        DoesNotExist,
        Rehydrating,
        Done
    }
    /// <summary> Persistent content-addressable store backed by Azure blobs. </summary>
    /// <see cref="AzureReadOnlyStore"/>
    /// <remarks>
    ///     Supports uploading blobs, as well as committing blobs uploaded to a staging
    ///     container.
    /// </remarks>
    public sealed class AzureStore : AzureReadOnlyStore, IAzureStore
    {
        /// <summary> Called when a new blob is committed. </summary>
        private readonly AzureWriter.OnCommit _onCommit;

        /// <summary> The container where temporary blobs are staged for a short while. </summary>
        private CloudBlobContainer Staging { get; }

        /// <summary> The container where archived blobs are stored. </summary>
        private CloudBlobContainer Archive { get; }

        /// <param name="realm"> <see cref="AzureReadOnlyStore"/> </param>
        /// <param name="persistent"> 
        ///     Blobs are stored here, named according to 
        ///     <see cref="AzureReadOnlyStore.AzureBlobName"/>.
        /// </param>
        /// <param name="staging"> Temporary blobs are stored here. </param>
        /// <param name="archive"> Archived blobs are stored here. </param>
        /// <param name="onCommit"> Called when a blob is committed. </param>
        public AzureStore(
            string realm,
            CloudBlobContainer persistent,
            CloudBlobContainer staging,
            CloudBlobContainer archive,
            AzureWriter.OnCommit onCommit = null) : base(realm, persistent)
        {
            _onCommit = onCommit;
            Staging = staging;
            Archive = archive;
        }

        /// <see cref="IStore{TBlobRef}.StartWriting"/>
        public StoreWriter StartWriting() =>
            new AzureWriter(Realm, Persistent, TempBlob(), _onCommit);

        /// <summary> A reference to a temporary blob in the staging container. </summary>
        private CloudBlockBlob TempBlob() =>
            Staging.GetBlockBlobReference(
                DateTime.UtcNow.ToString("yyyy-MM-dd") + "/" + Realm + "/" + Guid.NewGuid());

        /// <summary> Get the URL of a temporary blob where data can be uploaded. </summary>
        /// <remarks> 
        ///     Commit blob with <see cref="CommitTemporaryBlob"/>.
        /// 
        ///     The name should be a valid Azure Blob name, but its contents are not important
        ///     (although it is recommended that a "date/realm/guid" format is used to make
        ///     cleanup easier, to prevent cross-realm contamination, and to avoid collisions).
        /// </remarks>
        public Uri GetSignedUploadUrl(string name, TimeSpan life)
        {
            var blob = Staging.GetBlockBlobReference(name);
            var token = blob.GetSharedAccessSignature(new SharedAccessBlobPolicy
            {
                Permissions = SharedAccessBlobPermissions.Write | SharedAccessBlobPermissions.Delete,
                SharedAccessExpiryTime = new DateTimeOffset(DateTime.UtcNow + life)
            },
            new SharedAccessBlobHeaders
            {
                CacheControl = "private"
            });

            return new Uri(blob.Uri.AbsoluteUri + token);
        }

        /// <summary> Compress a blob into the archive container and set its tier to "archive" in Azure. </summary>
        /// <param name="blob"> The blob to be archived. </param>
        public async Task ArchiveBlobAsync(IAzureReadBlobRef blob)
        {
            var aBlob = await blob.GetBlob();
            var destinationCloudBlockBlob = Archive.GetBlockBlobReference(aBlob.Name);

            using (var azureWriteStream = await destinationCloudBlockBlob.OpenWriteAsync(CancellationToken.None))
            {
                using (var gzStream = new GZipStream(azureWriteStream, CompressionMode.Compress))
                {
                    using (var azureReadStream = await blob.OpenAsync(CancellationToken.None))
                    {
                        await azureReadStream.CopyToAsync(gzStream);
                    }
                }
            }
            await destinationCloudBlockBlob.SetStandardBlobTierAsync(StandardBlobTier.Archive);
        }

        public IAzureReadBlobRef GetAzureArchiveBlob(Hash hash) =>
            new AzureBlobRef(Realm, hash, Archive.GetBlockBlobReference(AzureBlobName(Realm, hash)));

        /// <summary>
        ///     UnArchive a blob. It's a long process split into several steps. First step is to move
        ///     the archived blob to the staging container and ask for its rehydratation.
        /// </summary>
        /// <remarks>
        ///     Rehydratation can take several hours, so come later and call this function again
        ///     to perform decompression into the persistent container.
        /// </remarks>
        /// <param name="hash"> The hash of the archived blob to be unarchived. </param>
        public async Task<UnArchiveStatus> TryUnArchiveBlobAsync(Hash hash)
        {
            // we check if the archived blob exists
            var aBlob = Archive.GetBlockBlobReference(AzureBlobName(Realm, hash));
            if (!(await aBlob.ExistsAsync(null, null, CancellationToken.None)))
                return UnArchiveStatus.DoesNotExist;

            // we check if UnArchive was already done successfully : if blob exists in Persistent
            var pBlob = Persistent.GetBlockBlobReference(AzureBlobName(Realm, hash));
            if (await pBlob.ExistsAsync(null, null, CancellationToken.None))
                return UnArchiveStatus.Done;
            
            var sBlob = Staging.GetBlockBlobReference(AzureBlobName(Realm, hash));
            // We check if Blob is already copied in Staging
            if (await sBlob.ExistsAsync(null, null, CancellationToken.None))
            {
                // We check which status it has
                if (sBlob.Properties.StandardBlobTier != StandardBlobTier.Hot)
                    return UnArchiveStatus.Rehydrating;

                var destinationCloudBlockBlob = Persistent.GetBlockBlobReference(sBlob.Name);
                // decompressing compressed blob into Persistent
                using (var azureReadStream = await sBlob.OpenReadAsync(CancellationToken.None))
                {
                    using (var gzStream = new GZipStream(azureReadStream, CompressionMode.Decompress))
                    {
                        WrittenBlob wBlob = await this.WriteAsync(gzStream, CancellationToken.None);
                        if(wBlob.Hash.ToString() != hash.ToString())
                            throw new Exception("Unarchive of '" + sBlob.Name + "' failed (hashes don't match)");
                    }
                }
                return UnArchiveStatus.Done;
            }
            // if blob not already copied in Staging,
            // copy it by using a newer API version that
            // deal with unarchiving at the same time
            var oc = new OperationContext();
            oc.SendingRequest += (s, e) =>
            {
                if (e.Request.Headers.Contains("x-ms-access-tier"))
                {
                    e.Request.Headers.Remove("x-ms-access-tier");
                }
                e.Request.Headers.Add("x-ms-access-tier", "Hot");
                e.Request.Headers.Add("x-ms-version", "2021-04-10");
            };
            await sBlob.StartCopyAsync(aBlob, null, null, null, oc, CancellationToken.None);
            
            return UnArchiveStatus.Rehydrating;
        }

        /// <summary> Commit a blob from staging to the persistent store. </summary>
        /// <remarks>
        ///     Computes the hash of the blob before committing it.
        /// </remarks>
        /// <param name="name"> The full name of the temporary blob. </param>
        /// <param name="cancel"> Cancellation token. </param>
        public async Task<IAzureReadBlobRef> CommitTemporaryBlob(string name, CancellationToken cancel) 
        {
            var sw = Stopwatch.StartNew();

            var temporary = Staging.GetBlockBlobReference(name);
            if (!await temporary.ExistsAsync(null, null, cancel).ConfigureAwait(false))
                throw new CommitBlobException(Realm, name, "temporary blob does not exist.");

            var md5 = MD5.Create();

            // We use buffered async reading, so determine a good buffer size.
            var bufferSize = 4 * 1024 * 1024;

            long? blobLength = temporary.Properties?.Length;
            if (blobLength < bufferSize)
                bufferSize = (int)blobLength.Value;

            var buffer = new byte[bufferSize];

            int nbRead = 1;
            long position = 0;

            using (var stream = await temporary.OpenReadAsync(null, null, null, cancel).ConfigureAwait(false))
            {
                int read = 0;
                do
                {
                    read = await stream.ReadAsync(buffer, 0, bufferSize, cancel)
                        .ConfigureAwait(false);

                    position += read;
                    nbRead++;

                    md5.TransformBlock(buffer, 0, read, buffer, 0);

                } while (read > 0);
            }

            md5.TransformFinalBlock(buffer, 0, 0);

            var hash = new Hash(md5.Hash);

            var final = Persistent.GetBlockBlobReference(AzureBlobName(Realm, hash));

            try
            {
                var exists = await AzureRetry.OrFalse(() => final.ExistsAsync(null, null, cancel))
                    .ConfigureAwait(false);

                if (!exists)
                {
                    await CopyToPersistent(temporary, final, cancel).ConfigureAwait(false);
                }

                _onCommit?.Invoke(sw.Elapsed, Realm, hash, final.Properties.Length, exists);
            }
            finally
            {
                // Always delete the blob.
                DeleteBlob(temporary, TimeSpan.FromMinutes(10));
            }

            return new AzureBlobRef(Realm, hash, final);
        }

        /// <summary> Delete a block after a short wait. </summary>
        /// <remarks> This schedules the deletion but does not wait for it. </remarks>
        public static void DeleteBlob(CloudBlockBlob temporary, TimeSpan wait)
        {
            // After a short while, delete the staging blob. Don't do it immediately, just
            // in case another thread (or server) is currently touching it as well.
            Task.Delay(wait).ContinueWith(_ => temporary.DeleteIfExistsAsync());
        }

        /// <summary> Copy a temporary blob to a persistent final blob. </summary>
        /// <remarks>
        ///     The task completes when the blob has been copied, or the copy has
        ///     failed. 
        /// </remarks>
        public static async Task CopyToPersistent(
            CloudBlockBlob temporary,
            CloudBlockBlob final,
            CancellationToken cancel)
        {
            // Copy the blob over.
            await AzureRetry.Do(
                c => final.StartCopyAsync(temporary, null, null, null, null, c),
                cancel).ConfigureAwait(false);

            // Wait for copy to finish.
            var delay = 250;
            while (true)
            {
                await AzureRetry.Do(
                    c => final.FetchAttributesAsync(null, null, null, c),
                    cancel).ConfigureAwait(false);

                switch (final.CopyState.Status)
                {
                    case CopyStatus.Pending:
                        if (delay <= 120000) delay *= 2;
                        await Task.Delay(TimeSpan.FromMilliseconds(delay), cancel);
                        continue;
                    case CopyStatus.Aborted:
                    case CopyStatus.Failed:
                    case CopyStatus.Invalid:
                        throw new Exception("Internal copy for '" + final.Name + "' failed (" + final.CopyState.Status + ")");
                    case CopyStatus.Success:
                        return;
                    default:
                        throw new ArgumentOutOfRangeException();
                }
            }
        }
    }
}
