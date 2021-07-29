using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace Lokad.ContentAddr.Azure
{
    /// <summary> A read-only content-addressable store backed by Azure Storage blobs. </summary>
    /// <remarks>
    ///     To avoid cross-account contamination, blobs are stored in *realms*, which are
    ///     prefixes: a blob with hash <c>H</c> in realm <c>R</c> is stored in the container 
    ///     under the blob name <c>R/H</c>.
    /// 
    ///     Each store is a window to a specific realm. 
    ///     Azure Storage Blobs may be stored on two different version of the storage. 
    ///     This store can deal with blobs from both versions, 
    ///     considering on as the new version and the other as the old version. 
    ///     All new blobs will be written on the new version, reading is performed on both, using the new version if possible. 
    ///     Blobs only existing on the old version will be copied to the new one when consulted.
    ///     
    /// </remarks>
    public class DualAzureReadOnlyStore : IAzureReadOnlyStore
    {
        /// <summary> The container where blobs are persisted on the old version. </summary>
        protected CloudBlobContainer OldPersistent { get; }

        /// <summary> The container where blobs are persisted on the new version. </summary>
        protected CloudBlobContainer NewPersistent { get; }

        /// <summary> The realm for this store. </summary>
        protected string Realm { get; }

        /// <see cref="IReadOnlyStore.Realm"/>
        long IReadOnlyStore.Realm => long.Parse(Realm);

        public DualAzureReadOnlyStore(string realm, CloudBlobContainer oldPersistent, CloudBlobContainer newPersistent)
        {
            OldPersistent = oldPersistent;
            NewPersistent = newPersistent;
            Realm = realm;
        }

        /// <see cref="IReadOnlyStore{TBlobRef}"/>
        public IAzureReadBlobRef this[Hash hash] =>
            new DualAzureBlobRef(
                Realm,
                hash,
                OldPersistent.GetBlockBlobReference(AzureBlobName(Realm, hash)),
                NewPersistent.GetBlockBlobReference(AzureBlobName(Realm, hash)));

        /// <summary>
        ///     Enumerate all blobs with the provided prefix, in ascending hash order per container,
        ///     invoking the callback with the hash, size and creation date of each blob.
        /// </summary>
        /// <remarks>As stated, the list will be built through the two stores, so
        /// globally the blobs will _not_ be listed in ascending order.</remarks>
        /// <returns> The number of blobs.</returns>
        public async Task<int> ListBlobsAsync(
            byte prefix,
            Action<Hash, long, DateTime> callback,
            CancellationToken cancel)
        {
            var blobPrefix = $"{Realm}/{prefix:X2}";
            var count = 0;

            foreach (var persistent in new[] { OldPersistent, NewPersistent })
            {
                var token = default(BlobContinuationToken);
                do
                {
                    var result = await persistent.ListBlobsSegmentedAsync(
                        prefix: blobPrefix,
                        useFlatBlobListing: true,
                        blobListingDetails: BlobListingDetails.Metadata,
                        maxResults: null,
                        currentToken: token,
                        options: null,
                        operationContext: null,
                        cancellationToken: cancel).ConfigureAwait(false);

                    foreach (var item in result.Results)
                    {
                        if (!(item is CloudBlob blob)) continue;
                        if (!Hash.TryParse(blob.Name.Substring(blob.Name.Length - 32), out var hash)) continue;
                        if (!(blob.Properties.LastModified is DateTimeOffset dto)) continue;

                        ++count;
                        callback(hash, blob.Properties.Length, dto.UtcDateTime);
                    }

                    token = result.ContinuationToken;

                } while (token != null);
            }

            return count;
        }

        IReadBlobRef IReadOnlyStore.this[Hash hash] => this[hash];

        /// <summary> Format the name of a block from its realm and content hash. </summary>
        public static string AzureBlobName(string realm, Hash hash) =>
            AzureReadOnlyStore.AzureBlobName(realm, hash);

        /// <summary> Format the name of a block from its account id and content hash. </summary>
        public static string AzureBlobName(long accountId, Hash hash) =>
            AzureReadOnlyStore.AzureBlobName(accountId, hash);

        public bool IsSameStore(IReadOnlyStore other)
        {
            if (other is DualAzureReadOnlyStore aros)
                return aros.NewPersistent.StorageUri.PrimaryUri.Equals(NewPersistent.StorageUri.PrimaryUri) &&
                       aros.OldPersistent.StorageUri.PrimaryUri.Equals(OldPersistent.StorageUri.PrimaryUri) &&
                       Realm == aros.Realm;

            return false;
        }

    }
}

