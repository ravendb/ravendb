using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests.Server.Replication;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.ServerWide;
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
        private class FakeIndex : AbstractIndexCreationTask
        {
            public override IndexDefinition CreateIndexDefinition()
            {
                return new IndexDefinition()
                {
                    Maps = { "from d in docs select new { d.Id }" }
                };
            }
        }

        [Fact]
        public void NewDisableAndEnableEndPointTest1()
        {
            using (var store = GetDocumentStore())
            {
                new FakeIndex().Execute(store);

                store.Maintenance.Send(new DisableIndexOperation("FakeIndex", false));

                var db = GetDocumentDatabaseInstanceFor(store).Result;
                var indexInstance = db.IndexStore.GetIndex("FakeIndex");

                Assert.Equal(IndexState.Disabled, indexInstance.State);

                store.Maintenance.Send(new EnableIndexOperation("FakeIndex", false));

                indexInstance = db.IndexStore.GetIndex("FakeIndex");

                Assert.Equal(IndexState.Normal, indexInstance.State);
            }
        }


        [Fact]
        public async Task NewDisableAndEnableEndPointTest2()
        {
            var (_, leader) = await CreateRaftCluster(3);
            var database = GetDatabaseName();
            await CreateDatabaseInClusterInner(new DatabaseRecord(database), 3, leader.WebUrl, null);
            var indexName = "SimpleIndex";
            DocumentDatabase documentDatabase = null;
            using (var store = new DocumentStore { Database = database, Urls = new[] { leader.WebUrl } }.Initialize())
            {
                await new SimpleIndex().ExecuteAsync(store);

                await store.Maintenance.SendAsync(new DisableIndexOperation(indexName, true));

                foreach (var server in Servers)
                {
                    await WaitForValueAsync(async () =>
                    {
                        documentDatabase = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(database);
                        return documentDatabase.IndexStore.GetIndex(indexName).State;
                    }, IndexState.Disabled);
                    Assert.Equal(IndexState.Disabled, documentDatabase.IndexStore.GetIndex(indexName).State);
                }

                await store.Maintenance.SendAsync(new EnableIndexOperation(indexName, true));

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


        [Fact]
        public async Task ChangeStaticIndexStateToDisable()
        {
            var (_, leader) = await CreateRaftCluster(3);
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
                WaitForUserToContinueTheTest(store);
                foreach (var server in Servers)
                {
                    await WaitForValueAsync(async () =>
                    {
                        documentDatabase = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(database);
                        return documentDatabase.IndexStore.GetIndex(indexName).State;
                    }, IndexState.Normal);
                    Assert.Equal(IndexState.Normal, documentDatabase.IndexStore.GetIndex(indexName).State);
                }

                await ActionWithLeader(async l =>
                {
                    var (index, _) = await l.ServerStore.Engine.PutAsync(new SetIndexStateCommand(indexName, IndexState.Disabled, database, Guid.NewGuid().ToString()));
                    await WaitForRaftIndexToBeAppliedInCluster(index, TimeSpan.FromSeconds(15));
                });

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
            var (_, leader) = await CreateRaftCluster(3);
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

                await ActionWithLeader(async l =>
                {
                    var (index, _) = await l.ServerStore.Engine.PutAsync(new SetIndexStateCommand(indexName, IndexState.Error, database, Guid.NewGuid().ToString()));
                    await WaitForRaftIndexToBeAppliedInCluster(index, TimeSpan.FromSeconds(15));
                });

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
            var (_, leader) = await CreateRaftCluster(3);
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

                await ActionWithLeader(async l =>
                {
                    var (index, _) = await l.ServerStore.Engine.PutAsync(new SetIndexStateCommand(indexName, IndexState.Idle, database, Guid.NewGuid().ToString()));
                    await WaitForRaftIndexToBeAppliedInCluster(index, TimeSpan.FromSeconds(15));
                });

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
            var (_, leader) = await CreateRaftCluster(3);
            var database = GetDatabaseName();
            await CreateDatabaseInClusterInner(new DatabaseRecord(database), 3, leader.WebUrl, null);
            var indexName = "Auto/Users/ByName";
            using (new DocumentStore
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

                await documentDatabase.IndexStore.CreateIndex(usersByName, Guid.NewGuid().ToString());

                foreach (var server in Servers)
                {
                    await WaitForValueAsync(async () =>
                    {
                        documentDatabase = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(database);
                        return documentDatabase.IndexStore.GetIndex(indexName).State;
                    }, IndexState.Normal);

                    Assert.Equal(IndexState.Normal, documentDatabase.IndexStore.GetIndex(indexName).State);
                }

                await ActionWithLeader(async l =>
                {
                    var (index, _) = await l.ServerStore.Engine.PutAsync(new SetIndexStateCommand(indexName, IndexState.Disabled, database, Guid.NewGuid().ToString()));
                    await WaitForRaftIndexToBeAppliedInCluster(index, TimeSpan.FromSeconds(15));
                });

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
            var (_, leader) = await CreateRaftCluster(3);
            var database = GetDatabaseName();
            await CreateDatabaseInClusterInner(new DatabaseRecord(database), 3, leader.WebUrl, null);
            var indexName = "Auto/Users/ByName";
            using (new DocumentStore
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

                await documentDatabase.IndexStore.CreateIndex(usersByName, Guid.NewGuid().ToString());

                foreach (var server in Servers)
                {
                    await WaitForValueAsync(async () =>
                    {
                        documentDatabase = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(database);
                        return documentDatabase.IndexStore.GetIndex(indexName).State;
                    }, IndexState.Normal);

                    Assert.Equal(IndexState.Normal, documentDatabase.IndexStore.GetIndex(indexName).State);
                }

                await ActionWithLeader(async l =>
                {
                    var (index, _) = await l.ServerStore.Engine.PutAsync(new SetIndexStateCommand(indexName, IndexState.Error, database, Guid.NewGuid().ToString()));
                    await WaitForRaftIndexToBeAppliedInCluster(index, TimeSpan.FromSeconds(15));
                });

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
            var (_, leader) = await CreateRaftCluster(3);
            var database = GetDatabaseName();
            await CreateDatabaseInClusterInner(new DatabaseRecord(database), 3, leader.WebUrl, null);
            var indexName = "Auto/Users/ByName";
            using (new DocumentStore
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

                await documentDatabase.IndexStore.CreateIndex(usersByName, Guid.NewGuid().ToString());

                foreach (var server in Servers)
                {
                    await WaitForValueAsync(async () =>
                    {
                        documentDatabase = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(database);
                        return documentDatabase.IndexStore.GetIndex(indexName).State;
                    }, IndexState.Normal);

                    Assert.Equal(IndexState.Normal, documentDatabase.IndexStore.GetIndex(indexName).State);
                }

                await ActionWithLeader(async l =>
                {
                    var (index, _) = await l.ServerStore.Engine.PutAsync(new SetIndexStateCommand(indexName, IndexState.Idle, database, Guid.NewGuid().ToString()));
                    await WaitForRaftIndexToBeAppliedInCluster(index, TimeSpan.FromSeconds(15));
                });

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
            var (_, leader) = await CreateRaftCluster(3);
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
                    }, IndexState.Normal, 3000);
                    Assert.Equal(IndexState.Normal, documentDatabase.IndexStore.GetIndex(indexName).State);
                }

                await ActionWithLeader(async l =>
                {
                    var (index, _) = await l.ServerStore.Engine.PutAsync(new SetIndexStateCommand(indexName, IndexState.Disabled, database, Guid.NewGuid().ToString()));
                    await WaitForRaftIndexToBeAppliedInCluster(index, TimeSpan.FromSeconds(15));
                });

                foreach (var server in Servers)
                {
                    await WaitForValueAsync(async () =>
                    {
                        documentDatabase = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(database);
                        return documentDatabase.IndexStore.GetIndex(indexName).State;
                    }, IndexState.Disabled, 3000);
                    Assert.Equal(IndexState.Disabled, documentDatabase.IndexStore.GetIndex(indexName).State);
                }

                await ActionWithLeader(async l =>
                {
                    var (index, _) = await l.ServerStore.Engine.PutAsync(new SetIndexStateCommand(indexName, IndexState.Normal, database, Guid.NewGuid().ToString()));
                    await WaitForRaftIndexToBeAppliedInCluster(index, TimeSpan.FromSeconds(15));
                });

                foreach (var server in Servers)
                {
                    await WaitForValueAsync(async () =>
                    {
                        documentDatabase = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(database);
                        return documentDatabase.IndexStore.GetIndex(indexName).State;
                    }, IndexState.Normal, 3000);
                    Assert.Equal(IndexState.Normal, documentDatabase.IndexStore.GetIndex(indexName).State);
                }

                await ActionWithLeader(async l =>
                {
                    var (index, _) = await l.ServerStore.Engine.PutAsync(new SetIndexStateCommand(indexName, IndexState.Error, database, Guid.NewGuid().ToString()));
                    await WaitForRaftIndexToBeAppliedInCluster(index, TimeSpan.FromSeconds(15));
                });

                foreach (var server in Servers)
                {
                    await WaitForValueAsync(async () =>
                    {
                        documentDatabase = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(database);
                        return documentDatabase.IndexStore.GetIndex(indexName).State;
                    }, IndexState.Error, 3000);
                    Assert.Equal(IndexState.Error, documentDatabase.IndexStore.GetIndex(indexName).State);
                }

                await ActionWithLeader(async l =>
                {
                    var (index, _) = await l.ServerStore.Engine.PutAsync(new SetIndexStateCommand(indexName, IndexState.Normal, database, Guid.NewGuid().ToString()));
                    await WaitForRaftIndexToBeAppliedInCluster(index, TimeSpan.FromSeconds(15));
                });

                foreach (var server in Servers)
                {
                    await WaitForValueAsync(async () =>
                    {
                        documentDatabase = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(database);
                        return documentDatabase.IndexStore.GetIndex(indexName).State;
                    }, IndexState.Normal, 3000);
                    Assert.Equal(IndexState.Normal, documentDatabase.IndexStore.GetIndex(indexName).State);
                }

                await ActionWithLeader(async l =>
                {
                    var (index, _) = await l.ServerStore.Engine.PutAsync(new SetIndexStateCommand(indexName, IndexState.Idle, database, Guid.NewGuid().ToString()));
                    await WaitForRaftIndexToBeAppliedInCluster(index, TimeSpan.FromSeconds(15));
                });

                foreach (var server in Servers)
                {
                    await WaitForValueAsync(async () =>
                    {
                        documentDatabase = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(database);
                        return documentDatabase.IndexStore.GetIndex(indexName).State;
                    }, IndexState.Idle, 3000);
                    Assert.Equal(IndexState.Idle, documentDatabase.IndexStore.GetIndex(indexName).State);
                }

                await ActionWithLeader(async l =>
                {
                    var (index, _) = await l.ServerStore.Engine.PutAsync(new SetIndexStateCommand(indexName, IndexState.Normal, database, Guid.NewGuid().ToString()));
                    await WaitForRaftIndexToBeAppliedInCluster(index, TimeSpan.FromSeconds(15));
                });

                foreach (var server in Servers)
                {
                    await WaitForValueAsync(async () =>
                    {
                        documentDatabase = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(database);
                        return documentDatabase.IndexStore.GetIndex(indexName).State;
                    }, IndexState.Normal, 3000);
                    Assert.Equal(IndexState.Normal, documentDatabase.IndexStore.GetIndex(indexName).State);
                }
            }
        }

        [Fact]
        public async Task LastSetStateDetemineTheState()
        {
            var (_, leader) = await CreateRaftCluster(3);
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

                documentDatabase = await Servers[0].ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(database);
                var index0 = documentDatabase.IndexStore.GetIndex(indexName);
                index0.SetState(IndexState.Idle);

                var count = 0;

                foreach (var server in Servers)
                {
                    documentDatabase = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(database);
                    if (documentDatabase.IndexStore.GetIndex(indexName).State == IndexState.Idle)
                        count++;
                }

                Assert.Equal(1, count);

                await ActionWithLeader((l) => l.ServerStore.Engine.PutAsync(new SetIndexStateCommand(indexName, IndexState.Disabled, database, Guid.NewGuid().ToString())),
                    Servers);

                foreach (var server in Servers)
                {
                    await WaitForValueAsync(async () =>
                    {
                        documentDatabase = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(database);
                        return documentDatabase.IndexStore.GetIndex(indexName).State;
                    }, IndexState.Disabled);
                    Assert.Equal(IndexState.Disabled, documentDatabase.IndexStore.GetIndex(indexName).State);
                }


                index0 = documentDatabase.IndexStore.GetIndex(indexName);
                index0.SetState(IndexState.Idle);

                count = 0;

                foreach (var server in Servers)
                {
                    await WaitForValueAsync(async () =>
                    {
                        documentDatabase = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(database);
                        return documentDatabase.IndexStore.GetIndex(indexName).State;
                    }, IndexState.Idle, 3000);
                    if (documentDatabase.IndexStore.GetIndex(indexName).State == IndexState.Idle)
                        count++;
                }

                Assert.Equal(1, count);
            }
        }

        [Fact]
        public async Task LocalStateDisabledCanBeChangedClusterWide()
        {
            var (_, leader) = await CreateRaftCluster(3);
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

                documentDatabase = await Servers[0].ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(database);
                var index0 = documentDatabase.IndexStore.GetIndex(indexName);
                index0.SetState(IndexState.Disabled);

                var count = 0;

                foreach (var server in Servers)
                {
                    documentDatabase = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(database);
                    if (documentDatabase.IndexStore.GetIndex(indexName).State == IndexState.Disabled)
                        count++;
                }

                Assert.Equal(1, count);

                await ActionWithLeader((l) => l.ServerStore.Engine.PutAsync(new SetIndexStateCommand(indexName, IndexState.Normal, database, Guid.NewGuid().ToString())),
                    Servers);

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

        [Fact]
        public async Task RavemDB_16982()
        {
            var indexName = "SimpleIndex";
            using (var store = GetDocumentStore())
            {
                await new SimpleIndex().ExecuteAsync(store);

                var documentDatabase = await Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
                var definition = documentDatabase.IndexStore.GetIndex(indexName).Definition;
                Assert.Equal(definition.State, IndexState.Normal);

                var compare = definition.Compare(new IndexDefinition() {Name = definition.Name, State = null, Maps = new HashSet<string>(){ "docs.Users.Select(user => new {\r\n    Name = user.Name\r\n})" } });
                Assert.Equal(compare, IndexDefinitionCompareDifferences.None);
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
