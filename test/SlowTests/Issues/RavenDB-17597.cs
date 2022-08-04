using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Net.Http.Json;
using System.Reflection;
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
using Raven.Client.Documents.Indexes.Spatial;
using Raven.Client.Documents.Linq;
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
using Raven.Client;

namespace SlowTests.Issues
{
    public class RavenDB_17597 : ClusterTestBase
    {
        public RavenDB_17597(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CopyToIndexDefinitionWorksProperly()
        {
            IndexDefinition a = GetNonDefaultIndexDefinition();


            var defaultIndexDefinition = new IndexDefinition();

            var differences = defaultIndexDefinition.Compare(a);

            Assert.Equal(IndexDefinitionCompareDifferences.All, differences);

            var fields = typeof(IndexDefinition).GetFields();
            var properties = typeof(IndexDefinition).GetProperties();

            var enumValues = Enum.GetValues<IndexDefinitionCompareDifferences>();

            Assert.Equal(enumValues.Length -2, //Exclude 'None' and 'All'
                fields.Length + properties.Length - 7
                //properties contains also 'Name', 'PatternReferencesCollectionName', 'PatternForOutputReduceToCollectionReferences', 'ReduceOutputIndex', 'OutputReduceToCollection', 'Type', 'SourceType'
                //which aren't contained in enumValues
                );

            IndexDefinition b = new IndexDefinition();
            a.CopyTo(b);

            differences = b.Compare(a);

            Assert.Equal(IndexDefinitionCompareDifferences.None, differences);
        }

        private IndexDefinition GetNonDefaultIndexDefinition()
        {
            var conf = new IndexConfiguration();
            conf.Add("key1", "val1");
            conf.Add("key2", "val2");


            var asm1 = AdditionalAssembly.FromNuGet("g", "123", "http://www.google.com", usings: new HashSet<string>() { "xx", "yy" });
            var asm2 = AdditionalAssembly.FromNuGet("e", "123", "http://www.ebay.com", usings: new HashSet<string>(){"xx","yy"});

            return new IndexDefinition()
            {
                Name = "a",
                Priority = IndexPriority.High,
                State = IndexState.Error,
                LockMode = IndexLockMode.LockedError,
                Reduce = "abc",
                SourceType = IndexSourceType.Counters,
                Type = IndexType.JavaScriptMapReduce,
                OutputReduceToCollection = "abcd",
                ReduceOutputIndex = 12345,
                PatternForOutputReduceToCollectionReferences = "abcde",
                PatternReferencesCollectionName = "abcdef",
                DeploymentMode = IndexDeploymentMode.Rolling,
                AdditionalSources = new Dictionary<string, string>() { { "a", "A" }, { "b", "B" } },
                AdditionalAssemblies = new HashSet<AdditionalAssembly>(){
                    asm1,
                    asm2,
                },
                Maps = new HashSet<string>() { "m1", "m2" },
                Fields = new Dictionary<string, IndexFieldOptions>()
                {
                    {"x", new IndexFieldOptions()
                        {
                            Analyzer = "x1",
                            Spatial = new SpatialOptions()
                            {
                                MaxX = 1,
                                MaxTreeLevel = 2
                            },
                            Suggestions = false,
                            Indexing = FieldIndexing.Exact,
                            Storage = FieldStorage.No,
                            TermVector = FieldTermVector.WithPositions
                        }
                    },
                    {"y", null}
                },
                Configuration = conf
            };
        }

        [Fact]
        public async Task SideBySideInRecordShouldBeFaulty()
        {
            using var server = GetNewServer(new ServerCreationOptions { RunInMemory = false, });
            using var store = GetDocumentStore(new Options { RunInMemory = false, Server = server });
            {
                // Prepare Server For Test
                for (int i = 0; i < 5; i++)
                    using (var session = store.OpenAsyncSession())
                    {
                        await session.StoreAsync(new Item { Num = 5 });
                        await session.SaveChangesAsync();
                    }

                // Wait for indexing in first node

                var index = new Index_ItemsByNum();
                index.Execute(store);
                Indexes.WaitForIndexing(store);
                WaitForUserToContinueTheTest(store);

                var database = await GetDatabase(server, store.Database);
                var record = new DatabaseRecord(store.Database);
                record.Indexes = new Dictionary<string, IndexDefinition>();
                record.AutoIndexes = new Dictionary<string, AutoIndexDefinition>();
                var sideBySideName = Constants.Documents.Indexing.SideBySideIndexNamePrefix + index.IndexName;
                record.Indexes[index.IndexName] = new IndexDefinition() { Name = sideBySideName };

                database.IndexStore.HandleDatabaseRecordChange(record, 0);

                var indexes = database.IndexStore.GetIndexes();
                Assert.NotNull(indexes.SingleOrDefault(i => i.Name == sideBySideName && i.Collections.Contains("@FaultyIndexes")));
            }
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
                    await new Index_ItemsByNum2().ExecuteAsync(store);
                    Indexes.WaitForIndexing(store);
                });
                var indexNames1 = store.Maintenance.Send(new GetIndexNamesOperation(0, 10));
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
                await WaitForIndexInitialization(newServer, store);
                var indexNames2 = store.Maintenance.Send(new GetIndexNamesOperation(0, 10));
                Assert.NotNull(indexNames2);
                Assert.Equal(2, indexNames2.Length);
                Assert.Contains("Items/ByNum", indexNames2);
                Assert.Contains("ReplacementOf/Items/ByNum", indexNames2);

                string[] indexNames3 = store.Maintenance.Send(new GetIndexErrorsOperation()).Select(x => x.Name).ToArray();
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

        private class Index_ItemsByNum : AbstractIndexCreationTask
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
        
        private class Index_ItemsByNum2 : AbstractIndexCreationTask
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

