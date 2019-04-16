using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FastTests;
using Raven.Server.Documents.PeriodicBackup.Azure;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Documents.PeriodicBackup
{
    public class Azure : NoDisposalNeeded
    {
        private const string AzureAccountName = "devstoreaccount1";
        private const string AzureAccountKey = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";

        [AzureStorageEmulatorFact]
        public async Task put_blob()
        {
            var containerName = Guid.NewGuid().ToString();
            var blobKey = Guid.NewGuid().ToString();

            using (var client = new RavenAzureClient(AzureAccountName, AzureAccountKey, containerName, isTest: true))
            {
                try
                {
                    await client.DeleteContainer();
                    await client.PutContainer();

                    await client.PutBlob(blobKey, new MemoryStream(Encoding.UTF8.GetBytes("123")), new Dictionary<string, string>
                    {
                        {"property1", "value1"},
                        {"property2", "value2"}
                    });
                    var blob = await client.GetBlob(blobKey);
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
                    await client.DeleteContainer();
                }
            }
        }

        [AzureStorageEmulatorFact]
        public async Task put_blob_in_folder()
        {
            var containerName = Guid.NewGuid().ToString();
            var blobKey = Guid.NewGuid().ToString();

            using (var client = new RavenAzureClient(AzureAccountName, AzureAccountKey, containerName, isTest: true))
            {
                try
                {
                    await client.DeleteContainer();
                    await client.PutContainer();

                    await client.PutBlob(blobKey, new MemoryStream(Encoding.UTF8.GetBytes("123")), new Dictionary<string, string>
                    {
                        {"property1", "value1"},
                        {"property2", "value2"}
                    });

                    var blob = await client.GetBlob(blobKey);
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
                    await client.DeleteContainer();
                }
            }
        }
    }
}
