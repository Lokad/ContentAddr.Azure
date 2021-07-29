using System;
using System.Threading;
using System.Threading.Tasks;

namespace Lokad.ContentAddr.Azure
{
    /// <summary> Persistent content-addressable store backed by Azure blobs. </summary>
    /// <see cref="AzureReadOnlyStore"/>
    /// <remarks>
    ///     Supports uploading blobs, as well as committing blobs uploaded to a staging
    ///     container.
    /// </remarks>
    public interface IAzureStore : IAzureReadOnlyStore, IStore<IAzureReadBlobRef>
    {

        Uri GetSignedUploadUrl(string name, TimeSpan life);

        /// <summary> Commit a blob from staging to the persistent store. </summary>
        /// <remarks>
        ///     Computes the hash of the blob before committing it.
        /// </remarks>
        /// <param name="name"> The full name of the temporary blob. </param>
        /// <param name="cancel"> Cancellation token. </param>
        Task<IAzureReadBlobRef> CommitTemporaryBlob(string name, CancellationToken cancel);
    }
}
