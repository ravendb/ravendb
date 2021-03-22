using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using FastTests;
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

        [AzureFact]
        public void list_blobs()
        {
            var blobNames = GenerateBlobNames(2, out var prefix);

            using (var client = new RavenAzureClient(AzureFactAttribute.AzureSettings))
            {
                try
                {
                    foreach (var blob in blobNames)
                    {
                        client.PutBlob(blob, new MemoryStream(Encoding.UTF8.GetBytes("abc")), new Dictionary<string, string>());
                    }

                    var blobsCount = client.ListBlobs(prefix, delimiter: null, listFolders: false).List.Count();
                    Assert.Equal(blobsCount, 2);
                }
                finally
                {
                    DeleteBlobs(client, blobNames, prefix);
                }
            }
        }

        [AzureFact]
        public void CanRemoveBlobsInBatch()
        {
            var blobKey = Guid.NewGuid().ToString();

            using var client = new RavenAzureClient(AzureFactAttribute.AzureSettings);
            var blobs = new List<string>();

            try
            {
                const string perfix = nameof(CanRemoveBlobsInBatch);

                for (int i = 0; i < 10; i++)
                {
                    var key = $"{perfix}/{blobKey}/northwind_{i}.ravendump";
                    var tmpArr = new byte[3];
                    new Random().NextBytes(tmpArr);
                    client.PutBlob(key, new MemoryStream(tmpArr), new Dictionary<string, string> {{$"property_{i}", $"value_{i}"}});

                    var blob = client.GetBlob(key);
                    Assert.NotNull(blob);

                    blobs.Add(key);
                }

                client.DeleteBlobs(blobs);

                var listBlobs = client.ListBlobs(perfix, null, listFolders: false);
                var blobNames = listBlobs.List.Select(b => b.Name).ToList();
                Assert.Equal(0, blobNames.Count);
            }
            finally
            {
                //TODO: delete blobs
            }
        }

        [AzureFact]
        public void RemoveNonExistingBlobsInBatchShouldThrow()
        {
            var containerName = Guid.NewGuid().ToString();
            var blobKey = Guid.NewGuid().ToString();

            using var client = new RavenAzureClient(AzureFactAttribute.AzureSettings);
            var blobs = new List<string>();
            try
            {
                client.DeleteContainer();
                client.PutContainer();

                // put blob
                var k = $"{blobKey}/northwind_322.ravendump";
                var tmpArr = new byte[3];
                new Random().NextBytes(tmpArr);
                client.PutBlob(k, new MemoryStream(tmpArr), new Dictionary<string, string> {{"Nice", "NotNice"}});

                var blob = client.GetBlob(k);
                Assert.NotNull(blob);
                blobs.Add(k);

                for (int i = 0; i < 10; i++)
                {
                    blobs.Add($"{blobKey}/northwind_{i}.ravendump");
                }

                try
                {
                    client.DeleteBlobs(blobs);
                }
                catch (Exception e)
                {
                    Assert.Equal(typeof(InvalidOperationException), e.GetType());
                    Assert.True(e.Message.StartsWith(
                        $"Failed to delete {blobs.Count - 1} blobs from container: {containerName}. Successfully deleted 1 blob. Reason: The specified blob does not exist."));
                }
            }
            finally
            {
                //TODO: delete blobs
            }
        }

        [AzureFact]
        public void put_blob()
        {
            var blobKey = Guid.NewGuid().ToString();

            using (var client = new RavenAzureClient(AzureFactAttribute.AzureSettings))
            {
                try
                {
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
                    DeleteBlobs(client, new List<string> { blobKey }, blobKey);
                }
            }
        }

        [AzureFact]
        public void put_blob_in_folder()
        {
            var blobNames = GenerateBlobNames(1, out var prefix);

            using (var client = new RavenAzureClient(AzureFactAttribute.AzureSettings))
            {
                try
                {
                    client.PutBlob(blobNames[0], new MemoryStream(Encoding.UTF8.GetBytes("123")),
                        new Dictionary<string, string> { { "property1", "value1" }, { "property2", "value2" } });

                    var blob = client.GetBlob(blobNames[0]);
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
                    DeleteBlobs(client, blobNames, prefix);
                }
            }
        }

        [AzureFact]
        public void put_blob_without_sas_token()
        {
            PutBlobs(5, false);
        }

        [AzureSasTokenFact]
        public void put_blob_with_sas_token()
        {
            PutBlobs(5, true);
        }

        private static void PutBlobs(int blobsCount, bool useSasToken)
        {
            var blobNames = GenerateBlobNames(blobsCount, out var prefix);

            using (var client = new RavenAzureClient(useSasToken ? AzureSasTokenFactAttribute.AzureSettings : AzureFactAttribute.AzureSettings))
            {
                for (var i = 0; i < blobsCount; i++)
                {
                    client.PutBlob(blobNames[i], new MemoryStream(Encoding.UTF8.GetBytes("123")),
                        new Dictionary<string, string> { { "property1", "value1" }, { "property2", "value2" } });
                }

                for (var i = 0; i < blobsCount; i++)
                {
                    var blob = client.GetBlob(blobNames[i]);
                    Assert.NotNull(blob);

                    using (var reader = new StreamReader(blob.Data))
                        Assert.Equal("123", reader.ReadToEnd());

                    var property1 = blob.Metadata.Keys.Single(x => x.Contains("property1"));
                    var property2 = blob.Metadata.Keys.Single(x => x.Contains("property2"));

                    Assert.Equal("value1", blob.Metadata[property1]);
                    Assert.Equal("value2", blob.Metadata[property2]);
                }

                var listBlobs = client.ListBlobs(prefix, null, listFolders: false);
                Assert.Equal(blobsCount, listBlobs.List.Count());

                // delete all blobs
                client.DeleteBlobs(blobNames);

                listBlobs = client.ListBlobs(prefix, null, listFolders: false);
                Assert.Equal(0, listBlobs.List.Select(b => b.Name).Count());

                for (var i = 0; i < blobsCount; i++)
                {
                    var blob = client.GetBlob(blobNames[i]);
                    Assert.Null(blob);
                }
            }
        }

        private static void DeleteBlobs(RavenAzureClient client, List<string> blobsToDelete, string prefix)
        {
            client.DeleteBlobs(blobsToDelete);

            var blobsCount = client.ListBlobs(prefix, delimiter: null, listFolders: false).List.Count();
            Assert.Equal(blobsCount, 0);
        }

        private static List<string> GenerateBlobNames(int blobsCount, out string prefix)
        {
            var blobNames = new List<string>();

            prefix = Guid.NewGuid().ToString();
            for (var i = 0; i < blobsCount; i++)
            {
                blobNames.Add($"{prefix}/{i}");
            }

            return blobNames;
        }
    }
}

