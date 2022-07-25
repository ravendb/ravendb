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
using Raven.Server.ServerWide.Context;
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

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task ModifyIndexThenRestartServer(bool stopIndex)
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
                new Index_ItemsByNum().Execute(store);
                Indexes.WaitForIndexing(store);
                if (stopIndex)
                {
                    store.Maintenance.Send(new StopIndexOperation("Items/ByNum"));
                }

                //Modify index
                await Assert.ThrowsAsync<InvalidOperationException>(async () =>
                {
                    new Index_ItemsByNum2().Execute(store);
                    Indexes.WaitForIndexing(store);
                });
                var indexNames1 = store.Maintenance.Send(new GetIndexNamesOperation(0, 10));
                Assert.NotNull(indexNames1);
                Assert.Equal(2, indexNames1.Length);
                Assert.Contains("Items/ByNum", indexNames1);
                Assert.Contains("ReplacementOf/Items/ByNum", indexNames1);

                // WaitForUserToContinueTheTest(store);

                //restart server
                var result = await DisposeServerAndWaitForFinishOfDisposalAsync(server);
                using var newServer = GetNewServer(new ServerCreationOptions
                {
                    DeletePrevious = false,
                    RunInMemory = false,
                    DataDirectory = result.DataDirectory,
                    CustomSettings = new Dictionary<string, string> { [RavenConfiguration.GetKey(x => x.Core.ServerUrls)] = result.Url }
                });
                await WaitForIndexInitialization(newServer, store);
                var indexNames2 = store.Maintenance.Send(new GetIndexNamesOperation(0, 10));
                Assert.NotNull(indexNames2);
                Assert.Equal(2, indexNames2.Length);
                Assert.Contains("Items/ByNum", indexNames2);
                Assert.Contains("ReplacementOf/Items/ByNum", indexNames2);

                // WaitForUserToContinueTheTest(store);

                string[] indexNames3 = null;
                using (var client = new HttpClient())
                {
                    Uri uri = new Uri(new Uri(server.WebUrl), $"/databases/{store.Database}/indexes/errors");
                    var response = await client.GetAsync(uri.ToString());
                    response.EnsureSuccessStatusCode();
                    ErrorsContent responseContent = await response.Content.ReadFromJsonAsync<ErrorsContent>();
                    indexNames3 = responseContent.GetIndexNames();
                }
                Assert.NotNull(indexNames3);
                Assert.Equal(2, indexNames3.Length);
                Assert.Contains("Items/ByNum", indexNames3);
                Assert.Contains("ReplacementOf/Items/ByNum", indexNames3);
            }
        }

        private async Task WaitForIndexInitialization(RavenServer server, DocumentStore store)
        {
            if (server.ServerStore.Initialized == false)
                await server.ServerStore.InitializationCompleted.WaitAsync();
            long lastRaftIndex;
            using (server.ServerStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext ctx))
            using (ctx.OpenReadTransaction())
            {
                lastRaftIndex = server.ServerStore.Engine.GetLastCommitIndex(ctx);
            }
            var database = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
            await database.RachisLogIndexNotifications.WaitForIndexNotification(lastRaftIndex, TimeSpan.FromSeconds(15));
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

