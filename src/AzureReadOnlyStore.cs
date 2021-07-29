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
    /// </remarks>
    public class AzureReadOnlyStore : IAzureReadOnlyStore
    {
        /// <summary> The container where blobs are persisted. </summary>
        protected CloudBlobContainer Persistent { get; }

        /// <summary> The realm for this store. </summary>
        protected string Realm { get; }

        /// <see cref="IReadOnlyStore.Realm"/>
        long IReadOnlyStore.Realm => long.Parse(Realm);

        public AzureReadOnlyStore(string realm, CloudBlobContainer persistent)
        {
            Persistent = persistent;
            Realm = realm;
        }

        /// <see cref="IReadOnlyStore{TBlobRef}"/>
        public IAzureReadBlobRef this[Hash hash] =>
            new AzureBlobRef(Realm, hash, Persistent.GetBlockBlobReference(AzureBlobName(Realm, hash)));

        IReadBlobRef IReadOnlyStore.this[Hash hash] => this[hash];

        /// <summary> Format the name of a block from its realm and content hash. </summary>
        public static string AzureBlobName(string realm, Hash hash) =>
            realm + "/" + hash;

        /// <summary> Format the name of a block from its account id and content hash. </summary>
        public static string AzureBlobName(long accountId, Hash hash) => AzureBlobName(accountId.ToString(), hash);

        public bool IsSameStore(IReadOnlyStore other)
        {
            if (other is AzureReadOnlyStore aros)
                return aros.Persistent.StorageUri.PrimaryUri.Equals(Persistent.StorageUri.PrimaryUri) &&
                       Realm == aros.Realm;

            return false;
        }

        /// <see cref="IAzureReadOnlyStore.ListBlobsAsync"/>
        public async Task<int> ListBlobsAsync(
            byte prefix,
            Action<Hash, long, DateTime> callback,
            CancellationToken cancel)
        {
            var blobPrefix = $"{Realm}/{prefix:X2}";
            var count = 0;

            var token = default(BlobContinuationToken);
            do
            {
                var result = await Persistent.ListBlobsSegmentedAsync(
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

            return count;
        }

        public async Task<bool> ListIfFewBlobsAsync(
            Action<Hash, long, DateTime> callback,
            CancellationToken cancel)
        {
            var blobPrefix = $"{Realm}/";

            var token = default(BlobContinuationToken);

            var result = await Persistent.ListBlobsSegmentedAsync(
                prefix: blobPrefix,
                useFlatBlobListing: true,
                blobListingDetails: BlobListingDetails.Metadata,
                maxResults: null,
                currentToken: token,
                options: null,
                operationContext: null,
                cancellationToken: cancel).ConfigureAwait(false);

            if (result.ContinuationToken != null) return false;

            foreach (var item in result.Results)
            {
                if (!(item is CloudBlob blob)) continue;
                if (!Hash.TryParse(blob.Name.Substring(blob.Name.Length - 32), out var hash)) continue;
                if (!(blob.Properties.LastModified is DateTimeOffset dto)) continue;

                callback(hash, blob.Properties.Length, dto.UtcDateTime);
            }

            return true;
        }
    }
}
