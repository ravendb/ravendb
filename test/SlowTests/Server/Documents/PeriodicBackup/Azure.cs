using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Operations.Backups;
using Raven.Server.Documents.PeriodicBackup.Azure;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.PeriodicBackup
{
    public class Azure : NoDisposalNeeded
    {
        public Azure(ITestOutputHelper output) : base(output)
        {
        }

        internal const string AzureAccountName = "devstoreaccount1";
        internal const string AzureAccountKey = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";

        [AzureStorageEmulatorFact(Skip = "Batch operations are not supported in emulator")]
        public void CanRemoveBlobsInBatch()
        {
            var containerName = Guid.NewGuid().ToString();
            var blobKey = Guid.NewGuid().ToString();

            using var client = new RavenAzureClient(GenerateAzureSettings(containerName));
            var blobs = new List<string>();
            try
            {
                client.DeleteContainer();
                client.PutContainer();

                for (int i = 0; i < 10; i++)
                {
                    var key = $"{blobKey}/northwind_{i}.ravendump";
                    var tmpArr = new byte[3];
                    new Random().NextBytes(tmpArr);
                    client.PutBlob(key, new MemoryStream(tmpArr), new Dictionary<string, string>
                    {
                        { $"property_{i}", $"value_{i}" }
                    });

                    var blob = client.GetBlob(key);
                    Assert.NotNull(blob);

                    blobs.Add(key);
                }

                client.DeleteMultipleBlobs(blobs);
            }
            finally
            {
                client.DeleteContainer();
            }
        }

        [AzureStorageEmulatorFact(Skip = "Batch operations are not supported in emulator")]
        public void RemoveNonExistingBlobsInBatchShouldThrow()
        {
            var containerName = Guid.NewGuid().ToString();
            var blobKey = Guid.NewGuid().ToString();

            using var client = new RavenAzureClient(GenerateAzureSettings(containerName));
            var blobs = new List<string>();
            try
            {
                client.DeleteContainer();
                client.PutContainer();

                // put blob
                var k = $"{blobKey}/northwind_322.ravendump";
                var tmpArr = new byte[3];
                new Random().NextBytes(tmpArr);
                client.PutBlob(k, new MemoryStream(tmpArr), new Dictionary<string, string>
                {
                    { "Nice", "NotNice" }
                });

                var blob = client.GetBlob(k);
                Assert.NotNull(blob);
                blobs.Add(k);

                for (int i = 0; i < 10; i++)
                {
                    blobs.Add($"{blobKey}/northwind_{i}.ravendump");
                }

                try
                {
                    client.DeleteMultipleBlobs(blobs);
                }
                catch (Exception e)
                {
                    Assert.Equal(typeof(InvalidOperationException), e.GetType());
                    Assert.True(e.Message.StartsWith($"Failed to delete {blobs.Count - 1} blobs from container: {containerName}. Successfully deleted 1 blob. Reason: The specified blob does not exist."));
                }
            }
            finally
            {
                client.DeleteContainer();
            }
        }

        [AzureStorageEmulatorFact]
        public void put_blob()
        {
            var containerName = Guid.NewGuid().ToString();
            var blobKey = Guid.NewGuid().ToString();

            using (var client = new RavenAzureClient(GenerateAzureSettings(containerName)))
            {
                try
                {
                    client.DeleteContainer();
                    client.PutContainer();

                    client.PutBlob(blobKey, new MemoryStream(Encoding.UTF8.GetBytes("123")), new Dictionary<string, string>
                    {
                        {"property1", "value1"},
                        {"property2", "value2"}
                    });
                    var blob = client.GetBlob(blobKey);
                    Assert.NotNull(blob);

                    using (var reader = new StreamReader(blob.Data))
                        Assert.Equal("123", reader.ReadToEnd());

                    var property1 = blob.Metadata.Keys.Single(x => x.Contains("property1"));
                    var property2 = blob.Metadata.Keys.Single(x => x.Contains("property2"));

                    Assert.Equal("value1", blob.Metadata[property1]);
                    Assert.Equal("value2", blob.Metadata[property2]);
                }
                finally
                {
                    client.DeleteContainer();
                }
            }
        }

        [AzureStorageEmulatorFact]
        public void put_blob_in_folder()
        {
            var containerName = Guid.NewGuid().ToString();
            var blobKey = Guid.NewGuid()+ "/" + Guid.NewGuid();

            using (var client = new RavenAzureClient(GenerateAzureSettings(containerName)))
            {
                try
                {
                    client.DeleteContainer();
                    client.PutContainer();

                    client.PutBlob(blobKey, new MemoryStream(Encoding.UTF8.GetBytes("123")), new Dictionary<string, string>
                    {
                        {"property1", "value1"},
                        {"property2", "value2"}
                    });

                    var blob = client.GetBlob(blobKey);
                    Assert.NotNull(blob);

                    using (var reader = new StreamReader(blob.Data))
                        Assert.Equal("123", reader.ReadToEnd());

                    var property1 = blob.Metadata.Keys.Single(x => x.Contains("property1"));
                    var property2 = blob.Metadata.Keys.Single(x => x.Contains("property2"));

                    Assert.Equal("value1", blob.Metadata[property1]);
                    Assert.Equal("value2", blob.Metadata[property2]);
                }
                finally
                {
                    client.DeleteContainer();
                }
            }
        }

        public static AzureSettings GenerateAzureSettings(string containerName)
        {
            return new AzureSettings
            {
                AccountName = AzureAccountName,
                AccountKey = AzureAccountKey,
                StorageContainer = containerName
            };
        }
    }
}
