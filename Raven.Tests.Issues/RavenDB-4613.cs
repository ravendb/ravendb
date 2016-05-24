// -----------------------------------------------------------------------
//  <copyright file="RavenDB_2181.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Database.Client.Azure;
using Raven.Tests.Common;
using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_4163 : NoDisposalNeeded
    {
        private const string AzureAccountName = "devstoreaccount1";
        private const string AzureAccountKey = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";

        [Fact(Skip = "Requires Windows Azure Development Storage")]
        public async Task put_blob_64MB()
        {
            var containerName = "testContainer";
            var blobKey = "testKey";

            using (var client = new RavenAzureClient(AzureAccountName, AzureAccountKey, containerName, isTest: true))
            {
                client.PutContainer();
                var sb = new StringBuilder();
                for (var i = 0; i < 64 * 1024 * 1024; i++)
                {
                    sb.Append("a");
                }

                await client.PutBlob(blobKey, new MemoryStream(Encoding.UTF8.GetBytes(sb.ToString())),
                    new Dictionary<string, string>
                    {
                        {"property1", "value1"},
                        {"property2", "value2"}
                    });

                var blob = client.GetBlob(blobKey);
                Assert.NotNull(blob);

                using (var reader = new StreamReader(blob.Data))
                    Assert.Equal(sb.ToString(), reader.ReadToEnd());

                var property1 = blob.Metadata.Keys.Single(x => x.Contains("property1"));
                var property2 = blob.Metadata.Keys.Single(x => x.Contains("property2"));

                Assert.Equal("value1", blob.Metadata[property1]);
                Assert.Equal("value2", blob.Metadata[property2]);
            }
        }

        [Fact(Skip = "Requires Windows Azure Development Storage")]
        public async Task put_blob_70MB()
        {
            var containerName = "testContainer";
            var blobKey = "testKey";

            using (var client = new RavenAzureClient(AzureAccountName, AzureAccountKey, containerName, isTest: true))
            {
                client.PutContainer();
                var sb = new StringBuilder();
                for (var i = 0; i < 70 * 1024 * 1024; i++)
                {
                    sb.Append("a");
                }

                await client.PutBlob(blobKey, new MemoryStream(Encoding.UTF8.GetBytes(sb.ToString())),
                    new Dictionary<string, string>
                    {
                        {"property1", "value1"},
                        {"property2", "value2"}
                    });

                var blob = client.GetBlob(blobKey);
                Assert.NotNull(blob);

                using (var reader = new StreamReader(blob.Data))
                    Assert.Equal(sb.ToString(), reader.ReadToEnd());

                var property1 = blob.Metadata.Keys.Single(x => x.Contains("property1"));
                var property2 = blob.Metadata.Keys.Single(x => x.Contains("property2"));

                Assert.Equal("value1", blob.Metadata[property1]);
                Assert.Equal("value2", blob.Metadata[property2]);
            }
        }

        [Fact(Skip = "Requires Windows Azure Development Storage")]
        public async Task put_blob_100MB()
        {
            var containerName = "testContainer";
            var blobKey = "testKey";

            using (var client = new RavenAzureClient(AzureAccountName, AzureAccountKey, containerName, isTest: true))
            {
                client.PutContainer();
                var sb = new StringBuilder();
                for (var i = 0; i < 100 * 1024 * 1024; i++)
                {
                    sb.Append("a");
                }

                await client.PutBlob(blobKey, new MemoryStream(Encoding.UTF8.GetBytes(sb.ToString())),
                    new Dictionary<string, string>
                    {
                        {"property1", "value1"},
                        {"property2", "value2"},
                        {"property3", "value3"}
                    });

                var blob = client.GetBlob(blobKey);
                Assert.NotNull(blob);

                using (var reader = new StreamReader(blob.Data))
                    Assert.Equal(sb.ToString(), reader.ReadToEnd());

                var property1 = blob.Metadata.Keys.Single(x => x.Contains("property1"));
                var property2 = blob.Metadata.Keys.Single(x => x.Contains("property2"));
                var property3 = blob.Metadata.Keys.Single(x => x.Contains("property3"));

                Assert.Equal("value1", blob.Metadata[property1]);
                Assert.Equal("value2", blob.Metadata[property2]);
                Assert.Equal("value3", blob.Metadata[property3]);
            }
        }

        [Fact(Skip = "Requires Windows Azure Development Storage")]
        public async Task put_blob_into_folder_64MB()
        {
            var containerName = "testContainer";
            var blobKey = "folder1/folder2/testKey";

            using (var client = new RavenAzureClient(AzureAccountName, AzureAccountKey, containerName, isTest: true))
            {
                client.PutContainer();

                var sb = new StringBuilder();
                for (var i = 0; i < 64 * 1024 * 1024; i++)
                {
                    sb.Append("a");
                }

                await client.PutBlob(blobKey, new MemoryStream(Encoding.UTF8.GetBytes(sb.ToString())),
                    new Dictionary<string, string>
                    {
                        {"property1", "value1"},
                        {"property2", "value2"}
                    });

                var blob = client.GetBlob(blobKey);
                Assert.NotNull(blob);

                using (var reader = new StreamReader(blob.Data))
                    Assert.Equal(sb.ToString(), reader.ReadToEnd());

                var property1 = blob.Metadata.Keys.Single(x => x.Contains("property1"));
                var property2 = blob.Metadata.Keys.Single(x => x.Contains("property2"));

                Assert.Equal("value1", blob.Metadata[property1]);
                Assert.Equal("value2", blob.Metadata[property2]);
            }
        }

        [Fact(Skip = "Requires Windows Azure Development Storage")]
        public async Task put_blob_into_folder_70MB()
        {
            var containerName = "testContainer";
            var blobKey = "folder1/folder2/testKey";

            using (var client = new RavenAzureClient(AzureAccountName, AzureAccountKey, containerName, isTest: true))
            {
                client.PutContainer();

                var sb = new StringBuilder();
                for (var i = 0; i < 70 * 1024 * 1024; i++)
                {
                    sb.Append("a");
                }

                await client.PutBlob(blobKey, new MemoryStream(Encoding.UTF8.GetBytes(sb.ToString())),
                    new Dictionary<string, string>
                    {
                        {"property1", "value1"},
                        {"property2", "value2"}
                    });

                var blob = client.GetBlob(blobKey);
                Assert.NotNull(blob);

                using (var reader = new StreamReader(blob.Data))
                    Assert.Equal(sb.ToString(), reader.ReadToEnd());

                var property1 = blob.Metadata.Keys.Single(x => x.Contains("property1"));
                var property2 = blob.Metadata.Keys.Single(x => x.Contains("property2"));

                Assert.Equal("value1", blob.Metadata[property1]);
                Assert.Equal("value2", blob.Metadata[property2]);
            }
        }

        [Fact(Skip = "Requires Windows Azure Development Storage")]
        public async Task put_blob_into_folder_100MB()
        {
            var containerName = "testContainer";
            var blobKey = "folder1/folder2/testKey";

            using (var client = new RavenAzureClient(AzureAccountName, AzureAccountKey, containerName, isTest: true))
            {
                client.PutContainer();

                var sb = new StringBuilder();
                for (var i = 0; i < 100 * 1024 * 1024; i++)
                {
                    sb.Append("a");
                }

                await client.PutBlob(blobKey, new MemoryStream(Encoding.UTF8.GetBytes(sb.ToString())),
                    new Dictionary<string, string>
                    {
                        {"property1", "value1"},
                        {"property2", "value2"},
                        {"property3", "value3"}
                    });

                var blob = client.GetBlob(blobKey);
                Assert.NotNull(blob);

                using (var reader = new StreamReader(blob.Data))
                    Assert.Equal(sb.ToString(), reader.ReadToEnd());

                var property1 = blob.Metadata.Keys.Single(x => x.Contains("property1"));
                var property2 = blob.Metadata.Keys.Single(x => x.Contains("property2"));
                var property3 = blob.Metadata.Keys.Single(x => x.Contains("property3"));

                Assert.Equal("value1", blob.Metadata[property1]);
                Assert.Equal("value2", blob.Metadata[property2]);
                Assert.Equal("value3", blob.Metadata[property3]);
            }
        }
    }
}
