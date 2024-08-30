using System;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.ServerWide;
using Raven.Server.Documents.Indexes;
using Raven.Server.ServerWide.Commands.Indexes;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;
using Raven.Server.Documents;
using Raven.Server.Documents.Indexes.MapReduce.Auto;
 
namespace SlowTests.Issues
{
    public class RavenDB_20922 : ClusterTestBase
    {
        public RavenDB_20922(ITestOutputHelper output) : base(output)
        {
        }
        private class TestObj
        {
            public string Id { get; set; }
            public string Prop { get; set; }
        }

        private static async Task<IndexDefinition[]> CreateAutoMapIndex(IDocumentStore store)
        {
            using (var session = store.OpenAsyncSession())
            {
                await session.Query<TestObj>().Where(x => x.Prop == "1").ToArrayAsync();
            }

            var index = await store.Maintenance.SendAsync(new GetIndexesOperation(0, 10));
            return index;
        }

        private static async Task<Raven.Server.Documents.Indexes.Index> CreateAutoMapReduceIndex(DocumentDatabase documentDatabase)
        {
            var usersByCountAndTotalAgeGroupedByLocation = new AutoMapReduceIndexDefinition("Users",
                new[]
                {
                    new AutoIndexField { Name = "Count", Storage = FieldStorage.Yes, Aggregation = AggregationOperation.Count, },
                    new AutoIndexField { Name = "TotalAge", Storage = FieldStorage.Yes, Aggregation = AggregationOperation.Sum },
                },
                new[] { new AutoIndexField { Name = "Location", Storage = FieldStorage.Yes, } });

            var index = await documentDatabase.IndexStore.CreateIndex(usersByCountAndTotalAgeGroupedByLocation, Guid.NewGuid().ToString());
            return index;
        }

        private static async Task EnableIndexClusterWide(DocumentStore store,string name)
        {
            await AssertWaitForValueAsync(async () =>
            {
                await store.Maintenance.SendAsync(new EnableIndexOperation(name, true));
                var indexDefinition = await store.Maintenance.SendAsync(new GetIndexOperation(name));
                return indexDefinition.State;
            }, IndexState.Normal);
        }

        private static async Task DisableIndexClusterWide(IDocumentStore store, string name)
        {
            await AssertWaitForValueAsync(async () =>
            {
                await store.Maintenance.SendAsync(new DisableIndexOperation(name, true));
                var indexDefinition = await store.Maintenance.SendAsync(new GetIndexOperation(name));
                return indexDefinition.State;
            }, IndexState.Disabled);
        }

        private async Task CheckIndexStateInTheCluster(string database, string name, IndexState state)
        {
            DocumentDatabase documentDatabase = null;
            foreach (var server in Servers)
            {
                await WaitForValueAsync(async () =>
                {
                    documentDatabase = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(database);
                    return documentDatabase.IndexStore.GetIndex(name).State;
                }, state);
                Assert.Equal(state, documentDatabase.IndexStore.GetIndex(name).State);
            }
        }

        [RavenFact(RavenTestCategory.Indexes)]
        public async Task DisableAutoMapIndexClusterWideAndEnableAutoMapIndexClusterWide()
        {
            const int numberOfNodes = 3;
            var (_, leader) = await CreateRaftCluster(numberOfNodes);
            using var store = GetDocumentStore(new Options { Server = leader, ReplicationFactor = numberOfNodes });

            IndexDefinition[] index = await CreateAutoMapIndex(store);
            WaitForUserToContinueTheTest(store);
            // Disable index cluster wide
            await DisableIndexClusterWide(store, index[0].Name);

            //Enable index cluster wide
            await EnableIndexClusterWide(store, index[0].Name);

            var documentDatabase = await leader.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
            var autoIndex = documentDatabase.IndexStore.GetIndex(index[0].Name);
            Assert.Equal(IndexState.Normal, autoIndex.State);
            Assert.Equal(IndexRunningStatus.Running, autoIndex.Status);
        }

        [RavenFact(RavenTestCategory.Indexes)]
        public async Task DisableAutoMapReduceIndexClusterWideAndEnableAutoMapReduceIndexClusterWide()
        {
            const int numberOfNodes = 3;
            var (nodes, leader) = await CreateRaftCluster(numberOfNodes);
            using var store = GetDocumentStore(new Options { Server = leader, ReplicationFactor = numberOfNodes });
            var documentDatabase = await leader.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);

            Raven.Server.Documents.Indexes.Index index = await CreateAutoMapReduceIndex(documentDatabase);

            await DisableIndexClusterWide(store, index.Name);
            foreach (var server in nodes)
            {
                await WaitForValueAsync(async () =>
                {
                    documentDatabase = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
                    return documentDatabase.IndexStore.GetIndex(index.Name).Status;
                }, IndexRunningStatus.Disabled);
                var autoIndex = documentDatabase.IndexStore.GetIndex(index.Name);
                Assert.Equal(IndexState.Disabled, autoIndex.State);
                Assert.Equal(IndexRunningStatus.Disabled, autoIndex.Status);
            }

            //Enable index cluster wide
            await EnableIndexClusterWide(store, index.Name);

            foreach (var server in nodes)
            {
                await WaitForValueAsync(async () =>
                {
                    documentDatabase = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
                    return documentDatabase.IndexStore.GetIndex(index.Name).Status;
                }, IndexRunningStatus.Running);
                var autoIndex = documentDatabase.IndexStore.GetIndex(index.Name);
                Assert.Equal(IndexState.Normal, autoIndex.State);
                Assert.Equal(IndexRunningStatus.Running, autoIndex.Status);
            }
        }

        [RavenFact(RavenTestCategory.Indexes)]
        public async Task LastSetStateDetermineTheStateAutoMapIndex()
        {
            var (_, leader) = await CreateRaftCluster(3);
            var database = GetDatabaseName();
            await CreateDatabaseInClusterInner(new DatabaseRecord(database), 3, leader.WebUrl, null);

            DocumentDatabase documentDatabase = null;
            using (var store = new DocumentStore
            {
                Database = database,
                Urls = new[] { leader.WebUrl }
            }.Initialize())
            {
                documentDatabase = await leader.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(database);
                IndexDefinition[] index = await CreateAutoMapIndex(store);

                //Check index is enabled and running
                foreach (var server in Servers)
                {
                    await CheckIndexStateInTheCluster(database, index[0].Name, IndexState.Normal);
                    await WaitForValueAsync(async () =>
                    {
                        documentDatabase = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(database);
                        return documentDatabase.IndexStore.GetIndex(index[0].Name).Status;
                    }, IndexRunningStatus.Running);
                    Assert.Equal(IndexRunningStatus.Running, documentDatabase.IndexStore.GetIndex(index[0].Name).Status);
                }

                documentDatabase = await Servers[0].ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(database);
                var autoIndex = documentDatabase.IndexStore.GetIndex(index[0].Name);

                var count = 0;
                string info = "";

                await ActionWithLeader((l) => l.ServerStore.Engine.SendToLeaderAsync(new SetIndexStateCommand(index[0].Name, IndexState.Disabled, database, Guid.NewGuid().ToString())),
                    Servers);
                //Check index is disabled
                await CheckIndexStateInTheCluster(database, index[0].Name, IndexState.Disabled);

                //Set index to normal
                autoIndex = documentDatabase.IndexStore.GetIndex(index[0].Name);
                autoIndex.SetState(IndexState.Normal);

                count = 0;

                await WaitForValueAsync(async () =>
                {
                    count = 0;
                    info = "";
                    foreach (var server in Servers)
                    {
                        documentDatabase = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(database);
                        var index2 = documentDatabase.IndexStore.GetIndex(index[0].Name);
                        var state = index2.State;
                        info += $"Index state for node {server.ServerStore.NodeTag} is {state} in definition {index2.Definition.State}.  ";
                        foreach (var error in index2.GetErrors())
                        {
                            info += $"{error.Error} , ";
                        }
                        if (state == IndexState.Normal)
                            count++;
                    }

                    return count;
                }, 1);

                Assert.True(1 == count, info);
            }
        }

        [RavenFact(RavenTestCategory.Indexes)]
        public async Task LastSetStateDetermineTheStateAutoMapReduceIndex()
        {
            var (_, leader) = await CreateRaftCluster(3);
            var database = GetDatabaseName();
            await CreateDatabaseInClusterInner(new DatabaseRecord(database), 3, leader.WebUrl, null);

            DocumentDatabase documentDatabase = null;
            using (var store = new DocumentStore
            {
                Database = database,
                Urls = new[] { leader.WebUrl }
            }.Initialize())
            {
                documentDatabase = await leader.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(database);
                // Create Auto map index
                Raven.Server.Documents.Indexes.Index index = await CreateAutoMapReduceIndex(documentDatabase);

                //Check index is enabled and running
                foreach (var server in Servers)
                {
                    await CheckIndexStateInTheCluster(database, index.Name, IndexState.Normal);
                    await WaitForValueAsync(async () =>
                    {
                        documentDatabase = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(database);
                        return documentDatabase.IndexStore.GetIndex(index.Name).Status;
                    }, IndexRunningStatus.Running);
                    Assert.Equal(IndexRunningStatus.Running, documentDatabase.IndexStore.GetIndex(index.Name).Status);
                }

                documentDatabase = await Servers[0].ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(database);
                var autoIndex = documentDatabase.IndexStore.GetIndex(index.Name);

                var count = 0;
                string info = "";

                await ActionWithLeader((l) => l.ServerStore.Engine.SendToLeaderAsync(new SetIndexStateCommand(index.Name, IndexState.Disabled, database, Guid.NewGuid().ToString())),
                    Servers);
                //Check index is disabled
                await CheckIndexStateInTheCluster(database, index.Name, IndexState.Disabled);

                //Set index to normal
                autoIndex = documentDatabase.IndexStore.GetIndex(index.Name);
                autoIndex.SetState(IndexState.Normal);

                count = 0;

                await WaitForValueAsync(async () =>
                {
                    count = 0;
                    info = "";
                    foreach (var server in Servers)
                    {
                        documentDatabase = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(database);
                        var index2 = documentDatabase.IndexStore.GetIndex(index.Name);
                        var state = index2.State;
                        info += $"Index state for node {server.ServerStore.NodeTag} is {state} in definition {index2.Definition.State}.  ";
                        foreach (var error in index2.GetErrors())
                        {
                            info += $"{error.Error} , ";
                        }
                        if (state == IndexState.Normal)
                            count++;
                    }

                    return count;
                }, 1);

                Assert.True(1 == count, info);
            }
        }

        [RavenFact(RavenTestCategory.Indexes)]
        public async Task ChangeLocallyAutoMapIndexStateToDisableAndEnableClusterWide()
        {
            var (_, leader) = await CreateRaftCluster(3);
            var database = GetDatabaseName();
            await CreateDatabaseInClusterInner(new DatabaseRecord(database), 3, leader.WebUrl, null);

            DocumentDatabase documentDatabase = null;
            using (var store = new DocumentStore
            {
                Database = database,
                Urls = new[] { leader.WebUrl }
            }.Initialize())
            {
                IndexDefinition[] index = await CreateAutoMapIndex(store);

                await CheckIndexStateInTheCluster(database, index[0].Name, IndexState.Normal);

                documentDatabase = await Servers[0].ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(database);
                var index0 = documentDatabase.IndexStore.GetIndex(index[0].Name);
                index0.SetState(IndexState.Disabled);

                var count = 0;

                foreach (var server in Servers)
                {
                    documentDatabase = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(database);
                    if (documentDatabase.IndexStore.GetIndex(index[0].Name).State == IndexState.Disabled)
                        count++;
                }

                Assert.Equal(1, count);

                await ActionWithLeader((l) => l.ServerStore.Engine.SendToLeaderAsync(new SetIndexStateCommand(index[0].Name, IndexState.Normal, database, Guid.NewGuid().ToString())),
                    Servers);

                await CheckIndexStateInTheCluster(database, index[0].Name, IndexState.Normal);
            }
        }

        [RavenFact(RavenTestCategory.Indexes)]
        public async Task ChangeLocallyAutoMapReduceIndexStateToDisableAndEnableClusterWide()
        {
            var (_, leader) = await CreateRaftCluster(3);
            var database = GetDatabaseName();
            await CreateDatabaseInClusterInner(new DatabaseRecord(database), 3, leader.WebUrl, null);

            DocumentDatabase documentDatabase = null;
            using (var store = new DocumentStore
            {
                Database = database,
                Urls = new[] { leader.WebUrl }
            }.Initialize())
            {
                documentDatabase = await Servers[0].ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(database);
                Raven.Server.Documents.Indexes.Index index = await CreateAutoMapReduceIndex(documentDatabase);

                await CheckIndexStateInTheCluster(database, index.Name, IndexState.Normal);

                documentDatabase = await Servers[0].ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(database);
                var index0 = documentDatabase.IndexStore.GetIndex(index.Name);
                index0.SetState(IndexState.Disabled);

                var count = 0;

                foreach (var server in Servers)
                {
                    documentDatabase = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(database);
                    if (documentDatabase.IndexStore.GetIndex(index.Name).State == IndexState.Disabled)
                        count++;
                }

                Assert.Equal(1, count);

                await ActionWithLeader((l) => l.ServerStore.Engine.SendToLeaderAsync(new SetIndexStateCommand(index.Name, IndexState.Normal, database, Guid.NewGuid().ToString())),
                    Servers);

                await CheckIndexStateInTheCluster(database, index.Name, IndexState.Normal);
            }
        }

        [RavenFact(RavenTestCategory.Indexes)]
        public async Task DisableAutoMapIndexClusterWideAndEnableLocally()
        {
            var (nodes, leader) = await CreateRaftCluster(3);
            var database = GetDatabaseName();
            await CreateDatabaseInClusterInner(new DatabaseRecord(database), 3, leader.WebUrl, null);

            DocumentDatabase documentDatabase = null;
            using (var store = new DocumentStore
            {
                Database = database,
                Urls = new[] { leader.WebUrl }
            }.Initialize())
            {
                IndexDefinition[] index = await CreateAutoMapIndex(store);

                await CheckIndexStateInTheCluster(database, index[0].Name, IndexState.Normal);

                await DisableIndexClusterWide(store, index[0].Name);

                foreach (var server in nodes)
                {
                    await WaitForValueAsync(async () =>
                    {
                        documentDatabase = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
                        return documentDatabase.IndexStore.GetIndex(index[0].Name).State;
                    }, IndexState.Disabled);
                    var autoIndex = documentDatabase.IndexStore.GetIndex(index[0].Name);
                    Assert.Equal(IndexState.Disabled, autoIndex.State);
                }

                documentDatabase = await Servers[0].ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(database);
                var index0 = documentDatabase.IndexStore.GetIndex(index[0].Name);
                index0.SetState(IndexState.Normal);

                var count = 0;

                foreach (var server in Servers)
                {
                    documentDatabase = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(database);
                    if (documentDatabase.IndexStore.GetIndex(index[0].Name).State == IndexState.Normal)
                        count++;
                }

                Assert.Equal(1, count);
            }
        }

        [RavenFact(RavenTestCategory.Indexes)]
        public async Task DisableAutoMapReduceIndexClusterWideAndEnableLocally()
        {
            var (_, leader) = await CreateRaftCluster(3);
            var database = GetDatabaseName();
            await CreateDatabaseInClusterInner(new DatabaseRecord(database), 3, leader.WebUrl, null);

            DocumentDatabase documentDatabase = null;
            using (var store = new DocumentStore
            {
                Database = database,
                Urls = new[] { leader.WebUrl }
            }.Initialize())
            {
                documentDatabase = await Servers[0].ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(database);
                Raven.Server.Documents.Indexes.Index index = await CreateAutoMapReduceIndex(documentDatabase);

                await CheckIndexStateInTheCluster(database, index.Name, IndexState.Normal);

                await DisableIndexClusterWide(store, index.Name);
                await CheckIndexStateInTheCluster(database, index.Name, IndexState.Disabled);

                documentDatabase = await Servers[0].ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(database);
                var index0 = documentDatabase.IndexStore.GetIndex(index.Name);
                index0.SetState(IndexState.Normal);

                var count = 0;

                foreach (var server in Servers)
                {
                    documentDatabase = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(database);
                    if (documentDatabase.IndexStore.GetIndex(index.Name).State == IndexState.Normal)
                        count++;
                }

                Assert.Equal(1, count);
            }
        }
    }
}
