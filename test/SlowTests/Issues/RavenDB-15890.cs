using System;
using System.Linq;
using System.Threading.Tasks;
using FastTests.Server.Replication;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.ServerWide;
using Raven.Client.Util;
using Raven.Server.Documents;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Indexes.Auto;
using Raven.Server.ServerWide.Commands.Indexes;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_15890 : ReplicationTestBase
    {
        public RavenDB_15890(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task ChangeStaticIndexStateToDisable()
        {
            var leader = await CreateRaftClusterAndGetLeader(3);
            var database = GetDatabaseName();
            await CreateDatabaseInClusterInner(new DatabaseRecord(database), 3, leader.WebUrl, null);
            var indexName = "SimpleIndex";
            DocumentDatabase documentDatabase = null;
            using (var store = new DocumentStore
            {
                Database = database,
                Urls = new[] { leader.WebUrl }
            }.Initialize())
            {
                await new SimpleIndex().ExecuteAsync(store);
                foreach (var server in Servers)
                {
                    await WaitForValueAsync(async () =>
                    {
                        documentDatabase = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(database);
                        return documentDatabase.IndexStore.GetIndex(indexName).State;
                    }, IndexState.Normal);
                    Assert.Equal(IndexState.Normal, documentDatabase.IndexStore.GetIndex(indexName).State);
                }

                var (index, _) = await leader.ServerStore.Engine.PutAsync(new SetIndexStateCommand(indexName, IndexState.Disabled, database, Guid.NewGuid().ToString()));

                await WaitForRaftIndexToBeAppliedInCluster(index, TimeSpan.FromSeconds(15));

                foreach (var server in Servers)
                {
                    await WaitForValueAsync(async () =>
                    {
                        documentDatabase = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(database);
                        return documentDatabase.IndexStore.GetIndex(indexName).State;
                    }, IndexState.Disabled);
                    Assert.Equal(IndexState.Disabled, documentDatabase.IndexStore.GetIndex(indexName).State);
                }
            }
        }

        [Fact]
        public async Task ChangeStaticIndexStateToError()
        {
            var leader = await CreateRaftClusterAndGetLeader(3);
            var database = GetDatabaseName();
            await CreateDatabaseInClusterInner(new DatabaseRecord(database), 3, leader.WebUrl, null);
            var indexName = "SimpleIndex";
            DocumentDatabase documentDatabase = null;
            using (var store = new DocumentStore
            {
                Database = database,
                Urls = new[] { leader.WebUrl }
            }.Initialize())
            {
                await new SimpleIndex().ExecuteAsync(store);
                foreach (var server in Servers)
                {
                    await WaitForValueAsync(async () =>
                    {
                        documentDatabase = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(database);
                        return documentDatabase.IndexStore.GetIndex(indexName).State;
                    }, IndexState.Normal);
                    Assert.Equal(IndexState.Normal, documentDatabase.IndexStore.GetIndex(indexName).State);
                }

                var (index, _) = await leader.ServerStore.Engine.PutAsync(new SetIndexStateCommand(indexName, IndexState.Error, database, Guid.NewGuid().ToString()));

                await WaitForRaftIndexToBeAppliedInCluster(index, TimeSpan.FromSeconds(15));

                foreach (var server in Servers)
                {
                    await WaitForValueAsync(async () =>
                    {
                        documentDatabase = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(database);
                        return documentDatabase.IndexStore.GetIndex(indexName).State;
                    }, IndexState.Error);
                    Assert.Equal(IndexState.Error, documentDatabase.IndexStore.GetIndex(indexName).State);
                }
            }
        }

        [Fact]
        public async Task ChangeStaticIndexStateToIdle()
        {
            var leader = await CreateRaftClusterAndGetLeader(3);
            var database = GetDatabaseName();
            await CreateDatabaseInClusterInner(new DatabaseRecord(database), 3, leader.WebUrl, null);
            var indexName = "SimpleIndex";
            DocumentDatabase documentDatabase = null;
            using (var store = new DocumentStore
            {
                Database = database,
                Urls = new[] { leader.WebUrl }
            }.Initialize())
            {
                await new SimpleIndex().ExecuteAsync(store);
                foreach (var server in Servers)
                {
                    await WaitForValueAsync(async () =>
                    {
                        documentDatabase = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(database);
                        return documentDatabase.IndexStore.GetIndex(indexName).State;
                    }, IndexState.Normal);
                    Assert.Equal(IndexState.Normal, documentDatabase.IndexStore.GetIndex(indexName).State);
                }

                var (index, _) = await leader.ServerStore.Engine.PutAsync(new SetIndexStateCommand(indexName, IndexState.Idle, database, Guid.NewGuid().ToString()));

                await WaitForRaftIndexToBeAppliedInCluster(index, TimeSpan.FromSeconds(15));

                foreach (var server in Servers)
                {
                    await WaitForValueAsync(async () =>
                    {
                        documentDatabase = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(database);
                        return documentDatabase.IndexStore.GetIndex(indexName).State;
                    }, IndexState.Idle);
                    Assert.Equal(IndexState.Idle, documentDatabase.IndexStore.GetIndex(indexName).State);
                }
            }
        }

        [Fact]
        public async Task ChangeAutoIndexStateToDisable()
        {
            var leader = await CreateRaftClusterAndGetLeader(3);
            var database = GetDatabaseName();
            await CreateDatabaseInClusterInner(new DatabaseRecord(database), 3, leader.WebUrl, null);
            var indexName = "Auto/Users/ByName";
            using (var store = new DocumentStore
            {
                Database = database,
                Urls = new[] { leader.WebUrl }
            }.Initialize())
            {
                var documentDatabase = await leader.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(database);
                var usersByName = new AutoMapIndexDefinition("Users", new[]
                {
                    new AutoIndexField
                    {
                        Name = "Name",
                        Storage = FieldStorage.No
                    },
                });
                AsyncHelpers.RunSync(() => documentDatabase.IndexStore.CreateIndex(usersByName, Guid.NewGuid().ToString()));
                foreach (var server in Servers)
                {
                    await WaitForValueAsync(async () =>
                    {
                        documentDatabase = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(database);
                        return documentDatabase.IndexStore.GetIndex(indexName).State;
                    }, IndexState.Normal);

                    Assert.Equal(IndexState.Normal, documentDatabase.IndexStore.GetIndex(indexName).State);
                }

                var (index, _) = await leader.ServerStore.Engine.PutAsync(new SetIndexStateCommand(indexName, IndexState.Disabled, database, Guid.NewGuid().ToString()));

                await WaitForRaftIndexToBeAppliedInCluster(index, TimeSpan.FromSeconds(15));

                foreach (var server in Servers)
                {
                    await WaitForValueAsync(async () =>
                    {
                        documentDatabase = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(database);
                        return documentDatabase.IndexStore.GetIndex(indexName).State;
                    }, IndexState.Disabled);
                    Assert.Equal(IndexState.Disabled, documentDatabase.IndexStore.GetIndex(indexName).State);
                }
            }
        }

        [Fact]
        public async Task ChangeAutoIndexStateToError()
        {
            var leader = await CreateRaftClusterAndGetLeader(3);
            var database = GetDatabaseName();
            await CreateDatabaseInClusterInner(new DatabaseRecord(database), 3, leader.WebUrl, null);
            var indexName = "Auto/Users/ByName";
            using (var store = new DocumentStore
            {
                Database = database,
                Urls = new[] { leader.WebUrl }
            }.Initialize())
            {
                var documentDatabase = await leader.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(database);
                var usersByName = new AutoMapIndexDefinition("Users", new[]
                {
                    new AutoIndexField
                    {
                        Name = "Name",
                        Storage = FieldStorage.No
                    },
                });
                AsyncHelpers.RunSync(() => documentDatabase.IndexStore.CreateIndex(usersByName, Guid.NewGuid().ToString()));
                foreach (var server in Servers)
                {
                    await WaitForValueAsync(async () =>
                    {
                        documentDatabase = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(database);
                        return documentDatabase.IndexStore.GetIndex(indexName).State;
                    }, IndexState.Normal);

                    Assert.Equal(IndexState.Normal, documentDatabase.IndexStore.GetIndex(indexName).State);
                }

                var (index, _) = await leader.ServerStore.Engine.PutAsync(new SetIndexStateCommand(indexName, IndexState.Error, database, Guid.NewGuid().ToString()));

                await WaitForRaftIndexToBeAppliedInCluster(index, TimeSpan.FromSeconds(15));

                foreach (var server in Servers)
                {
                    await WaitForValueAsync(async () =>
                    {
                        documentDatabase = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(database);
                        return documentDatabase.IndexStore.GetIndex(indexName).State;
                    }, IndexState.Error);
                    Assert.Equal(IndexState.Error, documentDatabase.IndexStore.GetIndex(indexName).State);
                }
            }
        }

        [Fact]
        public async Task ChangeAutoIndexStateToIdle()
        {
            var leader = await CreateRaftClusterAndGetLeader(3);
            var database = GetDatabaseName();
            await CreateDatabaseInClusterInner(new DatabaseRecord(database), 3, leader.WebUrl, null);
            var indexName = "Auto/Users/ByName";
            using (var store = new DocumentStore
            {
                Database = database,
                Urls = new[] { leader.WebUrl }
            }.Initialize())
            {
                var documentDatabase = await leader.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(database);
                var usersByName = new AutoMapIndexDefinition("Users", new[]
                {
                    new AutoIndexField
                    {
                        Name = "Name",
                        Storage = FieldStorage.No
                    },
                });
                AsyncHelpers.RunSync(() => documentDatabase.IndexStore.CreateIndex(usersByName, Guid.NewGuid().ToString()));
                foreach (var server in Servers)
                {
                    await WaitForValueAsync(async () =>
                    {
                        documentDatabase = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(database);
                        return documentDatabase.IndexStore.GetIndex(indexName).State;
                    }, IndexState.Normal);

                    Assert.Equal(IndexState.Normal, documentDatabase.IndexStore.GetIndex(indexName).State);
                }

                var (index, _) = await leader.ServerStore.Engine.PutAsync(new SetIndexStateCommand(indexName, IndexState.Idle, database, Guid.NewGuid().ToString()));

                await WaitForRaftIndexToBeAppliedInCluster(index, TimeSpan.FromSeconds(15));

                foreach (var server in Servers)
                {
                    await WaitForValueAsync(async () =>
                    {
                        documentDatabase = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(database);
                        return documentDatabase.IndexStore.GetIndex(indexName).State;
                    }, IndexState.Idle);
                    Assert.Equal(IndexState.Idle, documentDatabase.IndexStore.GetIndex(indexName).State);
                }
            }
        }

        [Fact]
        public async Task ChangeStaticIndexState()
        {
            var leader = await CreateRaftClusterAndGetLeader(3);
            var database = GetDatabaseName();
            await CreateDatabaseInClusterInner(new DatabaseRecord(database), 3, leader.WebUrl, null);
            var indexName = "SimpleIndex";
            DocumentDatabase documentDatabase= null;
            using (var store = new DocumentStore { Database = database, Urls = new[] { leader.WebUrl }
            }.Initialize())
            {
                await new SimpleIndex().ExecuteAsync(store);
                foreach (var server in Servers)
                {
                    await WaitForValueAsync(async () =>
                    {
                        documentDatabase = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(database);
                        return documentDatabase.IndexStore.GetIndex(indexName).State;
                    }, IndexState.Normal);
                    Assert.Equal(IndexState.Normal, documentDatabase.IndexStore.GetIndex(indexName).State);
                }

                var (index, _)  = await leader.ServerStore.Engine.PutAsync(new SetIndexStateCommand(indexName, IndexState.Disabled, database, Guid.NewGuid().ToString()));

                await WaitForRaftIndexToBeAppliedInCluster(index, TimeSpan.FromSeconds(15));

                foreach (var server in Servers)
                {
                    await WaitForValueAsync(async () =>
                    {
                        documentDatabase = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(database);
                        return documentDatabase.IndexStore.GetIndex(indexName).State;
                    }, IndexState.Disabled);
                    Assert.Equal(IndexState.Disabled, documentDatabase.IndexStore.GetIndex(indexName).State);
                }

                (index, _) = await leader.ServerStore.Engine.PutAsync(new SetIndexStateCommand(indexName, IndexState.Normal, database, Guid.NewGuid().ToString()));

                await WaitForRaftIndexToBeAppliedInCluster(index, TimeSpan.FromSeconds(15));

                foreach (var server in Servers)
                {
                    await WaitForValueAsync(async () =>
                    {
                        documentDatabase = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(database);
                        return documentDatabase.IndexStore.GetIndex(indexName).State;
                    }, IndexState.Normal);
                    Assert.Equal(IndexState.Normal, documentDatabase.IndexStore.GetIndex(indexName).State);
                }

                (index, _) = await leader.ServerStore.Engine.PutAsync(new SetIndexStateCommand(indexName, IndexState.Error, database, Guid.NewGuid().ToString()));

                await WaitForRaftIndexToBeAppliedInCluster(index, TimeSpan.FromSeconds(15));

                foreach (var server in Servers)
                {
                    await WaitForValueAsync(async () =>
                    {
                        documentDatabase = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(database);
                        return documentDatabase.IndexStore.GetIndex(indexName).State;
                    }, IndexState.Error);
                    Assert.Equal(IndexState.Error, documentDatabase.IndexStore.GetIndex(indexName).State);
                }

                (index, _) = await leader.ServerStore.Engine.PutAsync(new SetIndexStateCommand(indexName, IndexState.Normal, database, Guid.NewGuid().ToString()));

                await WaitForRaftIndexToBeAppliedInCluster(index, TimeSpan.FromSeconds(15));

                foreach (var server in Servers)
                {
                    await WaitForValueAsync(async () =>
                    {
                        documentDatabase = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(database);
                        return documentDatabase.IndexStore.GetIndex(indexName).State;
                    }, IndexState.Normal);
                    Assert.Equal(IndexState.Normal, documentDatabase.IndexStore.GetIndex(indexName).State);
                }

                (index, _) = await leader.ServerStore.Engine.PutAsync(new SetIndexStateCommand(indexName, IndexState.Idle, database, Guid.NewGuid().ToString()));

                await WaitForRaftIndexToBeAppliedInCluster(index, TimeSpan.FromSeconds(15));

                foreach (var server in Servers)
                {
                    await WaitForValueAsync(async () =>
                    {
                        documentDatabase = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(database);
                        return documentDatabase.IndexStore.GetIndex(indexName).State;
                    }, IndexState.Idle);
                    Assert.Equal(IndexState.Idle, documentDatabase.IndexStore.GetIndex(indexName).State);
                }

                (index, _) = await leader.ServerStore.Engine.PutAsync(new SetIndexStateCommand(indexName, IndexState.Normal, database, Guid.NewGuid().ToString()));

                await WaitForRaftIndexToBeAppliedInCluster(index, TimeSpan.FromSeconds(15));

                foreach (var server in Servers)
                {
                    await WaitForValueAsync(async () =>
                    {
                        documentDatabase = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(database);
                        return documentDatabase.IndexStore.GetIndex(indexName).State;
                    }, IndexState.Normal);
                    Assert.Equal(IndexState.Normal, documentDatabase.IndexStore.GetIndex(indexName).State);
                }
            }
        }


        private class SimpleIndex : AbstractIndexCreationTask<User>
        {
            public SimpleIndex()
            {
                Map = users => from user in users
                    select new { user.Name };
            }
        }

    }
}
