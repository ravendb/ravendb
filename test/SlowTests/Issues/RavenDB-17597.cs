using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Net.Http.Json;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using FastTests.Client;
using NuGet.ContentModel;
using Orders;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Session;
using Raven.Client.Exceptions.Database;
using Raven.Client.Exceptions.Documents.Indexes;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Server;
using Raven.Server.Commercial;
using Raven.Server.Config;
using Raven.Server.Documents;
using Raven.Server.Documents.Indexes;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;
using Xunit.Sdk;

namespace SlowTests.Issues
{
    public class RavenDB_17597 : ClusterTestBase
    {
        public RavenDB_17597(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task ModifyTaskIndexTest()
        {
            using var server = GetNewServer(new ServerCreationOptions { RunInMemory = false, });
            using var store = GetDocumentStore(new Options { RunInMemory = false, Server = server });
            {
                // Prepare Server For Test
                for(int i = 0; i < 5; i++)
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new Item
                    {
                        Num = 5
                    });
                    await session.SaveChangesAsync();
                }

                // Wait for indexing in first node
                var index = new Index_ItemsByNum();
                index.Execute(store);
                Indexes.WaitForIndexing(store);

                // Test
                CompactSettings settings = new CompactSettings { DatabaseName = store.Database, Documents = true, Indexes = new[] { index.IndexName } };

                //Modify index
                try
                {
                    var index2 = new Index_ItemsByNum2();
                    index2.Execute(store);
                    Indexes.WaitForIndexing(store);
                }
                catch(InvalidOperationException) { }
                string[] indexNames1 = store.Maintenance.Send(new GetIndexNamesOperation(0, 10));
                Assert.NotNull(indexNames1);
                Assert.Equal(2, indexNames1.Length);
                Assert.Contains("Items/ByNum", indexNames1);
                Assert.Contains("ReplacementOf/Items/ByNum", indexNames1);
                
                //restart server
                var result = await DisposeServerAndWaitForFinishOfDisposalAsync(server);
                using var newServer = GetNewServer(new ServerCreationOptions
                {
                    DeletePrevious = false,
                    RunInMemory = false,
                    DataDirectory = result.DataDirectory,
                    CustomSettings = new Dictionary<string, string> { [RavenConfiguration.GetKey(x => x.Core.ServerUrls)] = result.Url }
                });
                await Task.Delay(TimeSpan.FromSeconds(10)); // wait for ExecuteForIndexes call in IndexStore.HandleDatabaseRecordChange method.

                string[] indexNames = store.Maintenance.Send(new GetIndexNamesOperation(0, 10));
                Assert.NotNull(indexNames);
                Assert.Equal(2, indexNames.Length);
                Assert.Contains("Items/ByNum", indexNames);
                Assert.Contains("ReplacementOf/Items/ByNum", indexNames);

                string[] indexNames2 = null;
                using (var client = new HttpClient())
                {
                    Uri uri = new Uri(new Uri(server.WebUrl), "/databases/ModifyTaskIndexTest_1/indexes/errors");
                    var response = await client.GetAsync(uri.ToString());
                    response.EnsureSuccessStatusCode();
                    ErrorsContent responseContent = await response.Content.ReadFromJsonAsync<ErrorsContent>();
                    indexNames2 = responseContent.GetIndexNames();
                }
                Assert.NotNull(indexNames2);
                Assert.Equal(2, indexNames2.Length);
                Assert.Contains("Items/ByNum", indexNames2);
                Assert.Contains("ReplacementOf/Items/ByNum", indexNames2);
            }
        }

        private class Item
        {
            public string Id { get; set; }
            public int Num { get; set; }
        }

        private class ErrorsContent
        {
            public List<ErrorsContentResult> Results { get; set; }

            public class ErrorsContentResult
            {
                public string Name { get; set; }
            }

            public string[] GetIndexNames()
            {
                return Results.Select(x => x.Name).ToArray();
            }
        }

        public class Index_ItemsByNum : AbstractIndexCreationTask
        {
            public override string IndexName => "Items/ByNum";

            public override IndexDefinition CreateIndexDefinition()
            {
                return new IndexDefinition
                {
                    Maps =
                    {
                        @"docs.Items.Select(item => new {
                            Num = item.Num
                        })"
                    }
                };
            }
        }
        
        public class Index_ItemsByNum2 : AbstractIndexCreationTask
        {
            public override string IndexName => "Items/ByNum";

            public override IndexDefinition CreateIndexDefinition()
            {
                return new IndexDefinition
                {
                    Maps =
                    {
                        @"docs.Items.Select(item => new {
                            Num = item.Num/0
                        })"
                    }
                };
            }
        }

    }
}

