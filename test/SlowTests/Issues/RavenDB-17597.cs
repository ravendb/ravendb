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
            var conf = new IndexConfiguration();
            conf.Add("key1", "val1");
            conf.Add("key2", "val2");

            IndexDefinition a = GetNonDefaultIndexDefinition();

            // Assert 'a' doesn't have default  
            var defaultDefinition = new IndexDefinition();
            string[] defPropetisToIgnore = new string[] { "SourceType", "Type" }; // null's with exception.
            foreach (PropertyInfo property in a.GetType().GetProperties())
            {
                if (defPropetisToIgnore.Contains(property.Name))
                    continue;

                var value1 = property.GetValue(a, null);
                var value2 = property.GetValue(defaultDefinition, null);
                Assert.False(value1 == null && value2 == null, $"Property \"{property.Name}\" is equal - original: {value1}, default: {value2}");
                if (value1 == null || value2 == null)
                {
                    continue;
                }

                var t = Nullable.GetUnderlyingType(property.PropertyType);
                if (t == null)
                {
                    t = property.PropertyType;
                }
                dynamic c1 = Convert.ChangeType(value1, t);
                dynamic c2 = Convert.ChangeType(value2, t);

                Assert.False(c1 == c2, $"Property \"{property.Name}\" is different - equal: {c1}, default: {c2}");
            }

            // Assert a and b (clone made by CopyTo) are equals by values

            IndexDefinition b = new IndexDefinition();
            a.CopyTo(b);

            //AdditionalSources
            Assert.NotNull(a.AdditionalSources);
            Assert.NotNull(b.AdditionalSources);
            Assert.Equal(a.AdditionalSources.Count, b.AdditionalSources.Count);
            foreach (var kvp in a.AdditionalSources)
                Assert.Equal(kvp.Value, b.AdditionalSources[kvp.Key]);

            //AdditionalAssemblies
            Assert.NotNull(a.AdditionalAssemblies);
            Assert.NotNull(b.AdditionalAssemblies);
            Assert.Equal(a.AdditionalAssemblies.Count, b.AdditionalAssemblies.Count);
            foreach (var asm in a.AdditionalAssemblies)
                Assert.True(b.AdditionalAssemblies.SingleOrDefault(x => x.AssemblyName == asm.AssemblyName &&
                                                                                                x.AssemblyPath == asm.AssemblyPath &&
                                                                                                x.PackageName == asm.PackageName &&
                                                                                                x.PackageVersion == asm.PackageVersion &&
                                                                                                x.PackageSourceUrl == asm.PackageSourceUrl &&
                                                                                                StringHashSetEqualsByVal(x.Usings, asm.Usings)) != null
                                                                                    ,$"b.AdditionalAssemblies doesn't contain {asm}");

            //Maps
            Assert.NotNull(a.Maps);
            Assert.NotNull(b.Maps);
            Assert.Equal(a.Maps.Count, b.Maps.Count);
            foreach (var m in a.Maps)
                Assert.Contains(b.Maps, m2 => m2 == m);

            //Fields
            Assert.NotNull(a.Fields);
            Assert.NotNull(b.Fields);
            Assert.True(a.Fields.Count > 0);
            Assert.True(b.Fields.Count > 0);
            foreach (var kvp in a.Fields)
            {
                var value = kvp.Value;
                if (value == null)
                    continue;

                var bValue = b.Fields[kvp.Key];
                Assert.NotNull(bValue);

                Assert.Equal(value.Indexing, bValue.Indexing);
                Assert.Equal(value.Analyzer, bValue.Analyzer);
                Assert.Equal(value.Spatial, bValue.Spatial);
                Assert.Equal(value.Storage, bValue.Storage);
                Assert.Equal(value.Suggestions, bValue.Suggestions);
                Assert.Equal(value.TermVector, bValue.TermVector);
            }

            //Configuration
            Assert.NotNull(a.Configuration);
            Assert.NotNull(b.Configuration);
            Assert.True(a.Configuration.Count > 0);
            Assert.True(b.Configuration.Count > 0);
            foreach (var kvp in a.Configuration)
                Assert.Equal(kvp.Value, b.Configuration[kvp.Key]);

            var properties = a.GetType().GetProperties();
            string[] propetisToIgnore = new string[] { 
                "AdditionalSources", "AdditionalAssemblies", "Maps", "Fields", "Configuration", // collections - need to check it's elements
                "SourceType" }; // calculated on the fly
            foreach (PropertyInfo property in a.GetType().GetProperties())
            {
                if (propetisToIgnore.Contains(property.Name))
                    continue;

                var value1 = property.GetValue(a, null);
                var value2 = property.GetValue(b, null);
                if (value1 == null && value2 == null)
                {
                    continue;
                }
                Assert.False(value1 == null || value2 == null, $"Property \"{property.Name}\" is different - expected: {value1}, actual: {value2}");
                
                var t = Nullable.GetUnderlyingType(property.PropertyType);
                if (t == null)
                {
                    t = property.PropertyType;
                }
                dynamic c1 = Convert.ChangeType(value1, t);
                dynamic c2 = Convert.ChangeType(value2, t);

                Assert.True(c1 == c2, $"Property \"{property.Name}\" is different - expected: {c1}, actual: {c2}");
            }

            bool StringHashSetEqualsByVal(HashSet<string> h1, HashSet<string> h2)
            {
                if (h1 == h2)
                    return true;
                if(h1== null || h2 == null)
                    return false;
                if (h1.Count != h2.Count)
                    return false;

                foreach (var e in h1)
                {
                    if (h2.Contains(e) == false)
                        return false;
                }
                return true;
            }
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

