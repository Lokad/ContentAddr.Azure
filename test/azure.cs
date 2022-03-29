using Lokad.ContentAddr.Azure;
using Lokad.ContentAddr.Azure.Tests;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace Lokad.ContentAddr.Tests
{
    public class azure : UploadFixture, IDisposable
    {
        public static readonly string Connection = File.ReadAllText("azure_connection.txt");

        protected CloudBlobClient Client;

        protected CloudBlobContainer PersistContainer;

        protected CloudBlobContainer StagingContainer;

        protected CloudBlobContainer ArchiveContainer;

        protected string TestPrefix;

        public azure()
        {
            var account = CloudStorageAccount.Parse(Connection);
            Client = account.CreateCloudBlobClient();
            TestPrefix = Guid.NewGuid().ToString();

            PersistContainer = Client.GetContainerReference(TestPrefix + "-persist");
            PersistContainer.CreateIfNotExists();

            StagingContainer = Client.GetContainerReference(TestPrefix + "-staging");
            StagingContainer.CreateIfNotExists();

            ArchiveContainer = Client.GetContainerReference(TestPrefix + "-archive");
            ArchiveContainer.CreateIfNotExists();

            Store = new AzureStore("a", PersistContainer, StagingContainer, ArchiveContainer,
                (elapsed, realm, hash, size, existed) =>
                    Console.WriteLine("[{4}] {0} {1}/{2} {3} bytes", existed ? "OLD" : "NEW", realm, hash, size, elapsed));
        }

        public void Dispose()
        {
            try { PersistContainer.DeleteIfExists(); } catch { }
            try { StagingContainer.DeleteIfExists(); } catch { }
        }

        [Fact]
        public async Task small_file_azure()
        {
            var file = FakeFile(1024);
            var hash = Md5(file);
            var store = (AzureStore)Store;

            Assert.Equal("B2EA9F7FCEA831A4A63B213F41A8855B", hash.ToString());

            var r = await store.WriteAsync(file, CancellationToken.None);
            Assert.Equal("B2EA9F7FCEA831A4A63B213F41A8855B", r.Hash.ToString());
            Assert.Equal(1024, r.Size);

            var a = store[new Hash("B2EA9F7FCEA831A4A63B213F41A8855B")];
            var aBlob = await a.GetBlob();

            Assert.Equal("a/B2EA9F7FCEA831A4A63B213F41A8855B", aBlob.Name);
            Assert.True(await a.ExistsAsync(CancellationToken.None));

            Assert.Equal(1024, aBlob.Properties.Length);

            var url = (await a.GetDownloadUrlAsync(
                TimeSpan.FromMinutes(20),
                "test.bin",
                "application/octet-stream",
                CancellationToken.None)).ToString();

            var prefix = PersistContainer.Uri + "/a/B2EA9F7FCEA831A4A63B213F41A8855B";
            Assert.Equal(prefix, url.Substring(0, prefix.Length));

            var suffix = "&sp=r&rsct=application%2Foctet-stream&rscd=attachment%3Bfilename%3D\"test.bin\"";
            Assert.Equal(suffix, url.Substring(url.Length - suffix.Length));
        }

        [Fact(Skip = "Execute manually")]
        public async Task archive_azure()
        {
            var file = FakeFile(1024);
            var hash = Md5(file);
            var store = (AzureStore)Store;

            Assert.Equal("B2EA9F7FCEA831A4A63B213F41A8855B", hash.ToString());

            var r = await store.WriteAsync(file, CancellationToken.None);
            Assert.Equal("B2EA9F7FCEA831A4A63B213F41A8855B", r.Hash.ToString());
            Assert.Equal(1024, r.Size);

            var a = store[new Hash("B2EA9F7FCEA831A4A63B213F41A8855B")];
            var aBlob = await a.GetBlob();
            await store.ArchiveBlobAsync(a);
            await aBlob.DeleteAsync();
            await store.TryUnArchiveBlobAsync(new Hash("B2EA9F7FCEA831A4A63B213F41A8855B"));

            Boolean finished = false;
            while (!finished)
            {
                UnArchiveStatus status = await store.TryUnArchiveBlobAsync(new Hash("B2EA9F7FCEA831A4A63B213F41A8855B"));
                if (status == UnArchiveStatus.Done)
                {
                    var a2 = store[new Hash("B2EA9F7FCEA831A4A63B213F41A8855B")];
                    var a2Blob = await a2.GetBlob();
                    var stream = new AzureReadStream(a2Blob, file.Length);
                    foreach (var @byte in file)
                        Assert.Equal(@byte, stream.ReadByte());
                    finished = true;
                }
                else if (status == UnArchiveStatus.Rehydrating)
                    Console.WriteLine("Blob still rehydrating");
                Thread.Sleep(3000);
            }
        }

        [Fact]
        public async Task small_preserve_etag()
        {
            var file = FakeFile(1024);
            var store = (AzureStore)Store;

            var r = await store.WriteAsync(file, CancellationToken.None);
            var a = store[new Hash("B2EA9F7FCEA831A4A63B213F41A8855B")];
            var aBlob = await a.GetBlob();

            aBlob.FetchAttributes();
            var etag = aBlob.Properties.ETag;

            await store.WriteAsync(file, CancellationToken.None);

            var b = store[new Hash("B2EA9F7FCEA831A4A63B213F41A8855B")];
            var bBlob = await b.GetBlob();
            bBlob.FetchAttributes();
            Assert.Equal(etag, bBlob.Properties.ETag);
        }

        protected static byte[] StringToBytes(string str) => Encoding.UTF8.GetBytes(str);

        protected static string BytesToString(byte[] input) => Encoding.UTF8.GetString(input);

        protected static string BytesToHash(IEnumerable<byte> hash)
        {
            var sb = new StringBuilder();
            foreach (var t in hash)
                sb.AppendFormat("{0:X2}", t);
            return sb.ToString();
        }

        protected byte[] Read(string key)
        {
            var blob = PersistContainer.GetBlobReferenceFromServer(key);
            using (var stream = blob.OpenRead())
            {
                var ms = new MemoryStream();
                stream.CopyTo(ms);
                return ms.ToArray();
            }
        }

        [Theory]
        [InlineData(
            "filename.tsv", // Normal ASCII filename with no special characters
            "attachment%3Bfilename%3D\"filename.tsv\"")]
        [InlineData(
            "filenäme.tsv", // Non-ASCII character in filename
            "attachment%3Bfilename%3D\"data.tsv\"%3Bfilename%2A%3DUTF-8%27%27filen%25c3%25a4me.tsv")]
        public async Task ContentDisposition(string filename, string attach)
        {
            var account = CloudStorageAccount.Parse(Connection);
            var client = account.CreateCloudBlobClient();

            var persistContainer = Client.GetContainerReference("contentdisposition-persist");
            var stagingContainer = Client.GetContainerReference("contentdisposition-staging");
            var archiveContainer = Client.GetContainerReference("contentdisposition-archive");

            var store = new AzureStore("a", persistContainer, stagingContainer, archiveContainer);

            var blob = store[new Hash("B2EA9F7FCEA831A4A63B213F41A8855B")];
            var url = await blob.GetDownloadUrlAsync(
                new DateTime(2019, 9, 17, 21, 42, 0),
                TimeSpan.FromDays(10),
                filename,
                "text/plain",
                CancellationToken.None).ConfigureAwait(false);

            Assert.Contains(".blob.core.windows.net/contentdisposition-persist/a/B2EA9F7FCEA831A4A63B213F41A8855B", url.ToString());
            Assert.EndsWith(attach, url.ToString());
        }
    }
}
