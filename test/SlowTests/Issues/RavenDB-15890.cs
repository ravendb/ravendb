using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.ServerWide;
using Raven.Server.Documents;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Indexes.Auto;
using Raven.Server.ServerWide.Commands.Indexes;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
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
        public async Task DisableAndEnableLocallyEndPoint()
        {
            using (var store = GetDocumentStore())
            {
                new FakeIndex().Execute(store);

                store.Maintenance.Send(new DisableIndexOperation("FakeIndex", false));

                var db = await Databases.GetDocumentDatabaseInstanceFor(store);
                var indexInstance = db.IndexStore.GetIndex("FakeIndex");

                Assert.Equal(IndexState.Disabled, indexInstance.State);
                Assert.Equal(IndexRunningStatus.Disabled, indexInstance.Status);

                store.Maintenance.Send(new EnableIndexOperation("FakeIndex", false));

                indexInstance = db.IndexStore.GetIndex("FakeIndex");

                Assert.Equal(IndexState.Normal, indexInstance.State);
                Assert.Equal(IndexRunningStatus.Running, indexInstance.Status);
            }
        }


        [Fact]
        public async Task DisableAndEnableClusterWideEndPoint()
        {
            var (_, leader) = await CreateRaftCluster(3);
            var database = GetDatabaseName();
            await CreateDatabaseInClusterInner(new DatabaseRecord(database), 3, leader.WebUrl, null);
            var indexName = "SimpleIndex";
            DocumentDatabase documentDatabase = null;
            using (var store = new DocumentStore { Database = database, Urls = new[] { leader.WebUrl } }.Initialize())
            {
                var indexDefinition = new SimpleIndex().CreateIndexDefinition();
                indexDefinition.Name = indexName;
                
                var putIndexResults =  await store.Maintenance.SendAsync(new PutIndexesOperation(indexDefinition));
                await Cluster.WaitForRaftIndexToBeAppliedInClusterAsync(putIndexResults.First().RaftCommandIndex);
                
                await store.Maintenance.SendAsync(new DisableIndexOperation(indexName, true));

                foreach (var server in Servers)
                {
                    await WaitForValueAsync(async () =>
                    {
                        documentDatabase = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(database);
                        return documentDatabase.IndexStore.GetIndex(indexName).State;
                    }, IndexState.Disabled);
                    Assert.Equal(IndexState.Disabled, documentDatabase.IndexStore.GetIndex(indexName).State);
                    Assert.Equal(IndexRunningStatus.Disabled, documentDatabase.IndexStore.GetIndex(indexName).Status);
                }

                await store.Maintenance.SendAsync(new EnableIndexOperation(indexName, true));

                foreach (var server in Servers)
                {
                    await WaitForValueAsync(async () =>
                    {
                        documentDatabase = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(database);
                        return documentDatabase.IndexStore.GetIndex(indexName).Status;
                    }, IndexRunningStatus.Running);
                    var index = documentDatabase.IndexStore.GetIndex(indexName);
                    Assert.Equal(IndexState.Normal, index.State);
                    Assert.Equal(IndexRunningStatus.Running, index.Status);
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

                foreach (var server in Servers)
                {
                    await WaitForValueAsync(async () =>
                    {
                        documentDatabase = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(database);
                        return documentDatabase.IndexStore.GetIndex(indexName).State;
                    }, IndexState.Normal);
                    await WaitForValueAsync(async () =>
                    {
                        documentDatabase = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(database);
                        return documentDatabase.IndexStore.GetIndex(indexName).Status;
                    }, IndexRunningStatus.Running);
                    Assert.Equal(IndexState.Normal, documentDatabase.IndexStore.GetIndex(indexName).State);
                    Assert.Equal(IndexRunningStatus.Running, documentDatabase.IndexStore.GetIndex(indexName).Status);
                }

                await ActionWithLeader(async l =>
                {
                    var (index, _) = await l.ServerStore.Engine.PutAsync(new SetIndexStateCommand(indexName, IndexState.Disabled, database, Guid.NewGuid().ToString()));
                    await Cluster.WaitForRaftIndexToBeAppliedInClusterAsync(index, TimeSpan.FromSeconds(15));
                });

                foreach (var server in Servers)
                {
                    await WaitForValueAsync(async () =>
                    {
                        documentDatabase = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(database);
                        return documentDatabase.IndexStore.GetIndex(indexName).State;
                    }, IndexState.Disabled);
                    Assert.Equal(IndexState.Disabled, documentDatabase.IndexStore.GetIndex(indexName).State);
                    Assert.Equal(IndexRunningStatus.Disabled, documentDatabase.IndexStore.GetIndex(indexName).Status);
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
                    await WaitForValueAsync(async () =>
                    {
                        documentDatabase = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(database);
                        return documentDatabase.IndexStore.GetIndex(indexName).Status;
                    }, IndexRunningStatus.Running);
                    Assert.Equal(IndexState.Normal, documentDatabase.IndexStore.GetIndex(indexName).State);

                    await WaitForValueAsync(async () =>
                    {
                        documentDatabase = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(database);
                        return documentDatabase.IndexStore.GetIndex(indexName).Status;
                    }, IndexRunningStatus.Running);
                    
                    Assert.Equal(IndexRunningStatus.Running, documentDatabase.IndexStore.GetIndex(indexName).Status);
                }

                await ActionWithLeader(async l =>
                {
                    var (index, _) = await l.ServerStore.Engine.PutAsync(new SetIndexStateCommand(indexName, IndexState.Error, database, Guid.NewGuid().ToString()));
                    await Cluster.WaitForRaftIndexToBeAppliedInClusterAsync(index, TimeSpan.FromSeconds(15));
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
                        return documentDatabase.IndexStore.GetIndex(indexName).Status;
                    }, IndexRunningStatus.Running);
                    var autoIndex = documentDatabase.IndexStore.GetIndex(indexName);
                    Assert.Equal(IndexState.Normal, autoIndex.State);
                    Assert.Equal(IndexRunningStatus.Running, autoIndex.Status);
                }

                await ActionWithLeader(async l =>
                {
                    var (index, _) = await l.ServerStore.Engine.PutAsync(new SetIndexStateCommand(indexName, IndexState.Disabled, database, Guid.NewGuid().ToString()));
                    await Cluster.WaitForRaftIndexToBeAppliedInClusterAsync(index, TimeSpan.FromSeconds(15));
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
                    await Cluster.WaitForRaftIndexToBeAppliedInClusterAsync(index, TimeSpan.FromSeconds(15));
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
                    await Cluster.WaitForRaftIndexToBeAppliedInClusterAsync(index, TimeSpan.FromSeconds(15));
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
                    await WaitForValueAsync(async () =>
                    {
                        documentDatabase = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(database);
                        return documentDatabase.IndexStore.GetIndex(indexName).Status;
                    }, IndexRunningStatus.Running);
                    Assert.Equal(IndexState.Normal, documentDatabase.IndexStore.GetIndex(indexName).State);
                    Assert.Equal(IndexRunningStatus.Running, documentDatabase.IndexStore.GetIndex(indexName).Status);
                }

                await ActionWithLeader(async l =>
                {
                    var (index, _) = await l.ServerStore.Engine.PutAsync(new SetIndexStateCommand(indexName, IndexState.Disabled, database, Guid.NewGuid().ToString()));
                    await Cluster.WaitForRaftIndexToBeAppliedInClusterAsync(index, TimeSpan.FromSeconds(15));
                });

                foreach (var server in Servers)
                {
                    await WaitForValueAsync(async () =>
                    {
                        documentDatabase = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(database);
                        return documentDatabase.IndexStore.GetIndex(indexName).State;
                    }, IndexState.Disabled, 3000);
                    await WaitForValueAsync(async () =>
                    {
                        documentDatabase = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(database);
                        return documentDatabase.IndexStore.GetIndex(indexName).Status;
                    }, IndexRunningStatus.Disabled);
                    Assert.Equal(IndexState.Disabled, documentDatabase.IndexStore.GetIndex(indexName).State);
                    Assert.Equal(IndexRunningStatus.Disabled, documentDatabase.IndexStore.GetIndex(indexName).Status);
                }

                await ActionWithLeader(async l =>
                {
                    var (index, _) = await l.ServerStore.Engine.PutAsync(new SetIndexStateCommand(indexName, IndexState.Normal, database, Guid.NewGuid().ToString()));
                    await Cluster.WaitForRaftIndexToBeAppliedInClusterAsync(index, TimeSpan.FromSeconds(15));
                });

                foreach (var server in Servers)
                {
                    await WaitForValueAsync(async () =>
                    {
                        documentDatabase = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(database);
                        return documentDatabase.IndexStore.GetIndex(indexName).State;
                    }, IndexState.Normal, 3000);
                    await WaitForValueAsync(async () =>
                    {
                        documentDatabase = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(database);
                        return documentDatabase.IndexStore.GetIndex(indexName).Status;
                    }, IndexRunningStatus.Running);
                    Assert.Equal(IndexState.Normal, documentDatabase.IndexStore.GetIndex(indexName).State);
                    Assert.Equal(IndexRunningStatus.Running, documentDatabase.IndexStore.GetIndex(indexName).Status);
                }

                await ActionWithLeader(async l =>
                {
                    var (index, _) = await l.ServerStore.Engine.PutAsync(new SetIndexStateCommand(indexName, IndexState.Error, database, Guid.NewGuid().ToString()));
                    await Cluster.WaitForRaftIndexToBeAppliedInClusterAsync(index, TimeSpan.FromSeconds(15));
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
                    await Cluster.WaitForRaftIndexToBeAppliedInClusterAsync(index, TimeSpan.FromSeconds(15));
                });

                foreach (var server in Servers)
                {
                    await WaitForValueAsync(async () =>
                    {
                        documentDatabase = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(database);
                        return documentDatabase.IndexStore.GetIndex(indexName).State;
                    }, IndexState.Normal, 3000);
                    await WaitForValueAsync(async () =>
                    {
                        documentDatabase = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(database);
                        return documentDatabase.IndexStore.GetIndex(indexName).Status;
                    }, IndexRunningStatus.Running);
                    Assert.Equal(IndexState.Normal, documentDatabase.IndexStore.GetIndex(indexName).State);
                    Assert.Equal(IndexRunningStatus.Running, documentDatabase.IndexStore.GetIndex(indexName).Status);
                }
            }
        }

        [Fact]
        public async Task LastSetStateDetermineTheState()
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
                    await WaitForValueAsync(async () =>
                    {
                        documentDatabase = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(database);
                        return documentDatabase.IndexStore.GetIndex(indexName).Status;
                    }, IndexRunningStatus.Running);
                    Assert.Equal(IndexState.Normal, documentDatabase.IndexStore.GetIndex(indexName).State);
                    Assert.Equal(IndexRunningStatus.Running, documentDatabase.IndexStore.GetIndex(indexName).Status);
                }

                documentDatabase = await Servers[0].ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(database);
                var index0 = documentDatabase.IndexStore.GetIndex(indexName);

                var count = 0;
                string info = "";
               
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
                index0.SetState(IndexState.Normal);

                count = 0;

                await WaitForValueAsync(async () =>
                {
                    count = 0;
                    info = "";
                    foreach (var server in Servers)
                    {
                        documentDatabase = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(database);
                        var index = documentDatabase.IndexStore.GetIndex(indexName);
                        var state = index.State;
                        info += $"Index state for node {server.ServerStore.NodeTag} is {state} in definition {index.Definition.State}.  ";
                        foreach (var error in index.GetErrors())
                        {
                            info += $"{error.Error} , ";
                        }
                        if ( state == IndexState.Normal)
                            count++;
                    }

                    return count;
                }, 1);

                Assert.True(1 == count, info);
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
        public async Task IndexDefinitionCompareState_RavenDB_16982()
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

        [Fact]
        public async Task UpdateDefinitionWithoutState()
        {
            var (_, leader) = await CreateRaftCluster(3, watcherCluster: true);
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
                //Start with normal
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

                // Disable index cluster wide
                var (index, _) = await leader.ServerStore.Engine.PutAsync(new SetIndexStateCommand(indexName, IndexState.Disabled, database, Guid.NewGuid().ToString()));

                await Cluster.WaitForRaftIndexToBeAppliedInClusterAsync(index, TimeSpan.FromSeconds(15));

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
                //Change priority cluster wide
                (index, _) = await leader.ServerStore.Engine.PutAsync(new SetIndexPriorityCommand(indexName, IndexPriority.Low, database, Guid.NewGuid().ToString()));
                await Cluster.WaitForRaftIndexToBeAppliedInClusterAsync(index, TimeSpan.FromSeconds(15));

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
        public async Task UpdateDefinitionWithoutState2()
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
                //Start with normal
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
                // Disable index cluster wide
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
                //enable locally
                await store.Maintenance.SendAsync(new EnableIndexOperation(indexName, false));
                var count = 0;

                foreach (var server in Servers)
                {
                    documentDatabase = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(database);
                    if (documentDatabase.IndexStore.GetIndex(indexName).State == IndexState.Disabled)
                        count++;
                    else
                    {
                        Assert.Equal(IndexRunningStatus.Running, documentDatabase.IndexStore.GetIndex(indexName).Status);
                    }
                }
                Assert.Equal(2, count);
                //Change priority cluster wide
                var (index, _) = await leader.ServerStore.Engine.PutAsync(new SetIndexPriorityCommand(indexName, IndexPriority.Low, database, Guid.NewGuid().ToString()));

                await Cluster.WaitForRaftIndexToBeAppliedInClusterAsync(index, TimeSpan.FromSeconds(15));
                count = 0;
                foreach (var server in Servers)
                {
                    documentDatabase = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(database);
                    if (documentDatabase.IndexStore.GetIndex(indexName).State == IndexState.Disabled)
                        count++;
                    else
                    {
                        Assert.Equal(IndexRunningStatus.Running, documentDatabase.IndexStore.GetIndex(indexName).Status);
                    }
                }
                Assert.Equal(2, count);
            }
        }

        [Fact]
        public async Task ClusterWideEnableAfterPause()
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
                //Start with normal
                await new SimpleIndex().ExecuteAsync(store);

                documentDatabase = await leader.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(database);
                var res = WaitForValue(() =>
                {
                    var ind = documentDatabase.IndexStore.GetIndex(indexName);
                    return ind != null;
                }, true);
                Assert.True(res);
                documentDatabase.IndexStore.StopIndex(indexName);
                Assert.Equal(IndexRunningStatus.Paused, documentDatabase.IndexStore.GetIndex(indexName).Status);

                var (index, _) = await leader.ServerStore.Engine.PutAsync(new SetIndexStateCommand(indexName, IndexState.Disabled, database, Guid.NewGuid().ToString()));

                await Cluster.WaitForRaftIndexToBeAppliedInClusterAsync(index, TimeSpan.FromSeconds(15));

                documentDatabase = await leader.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(database);
                Assert.Equal(IndexState.Disabled, documentDatabase.IndexStore.GetIndex(indexName).State);

                // Enable index cluster wide
                (index, _) = await leader.ServerStore.Engine.PutAsync(new SetIndexStateCommand(indexName, IndexState.Normal, database, Guid.NewGuid().ToString()));

                await Cluster.WaitForRaftIndexToBeAppliedInClusterAsync(index, TimeSpan.FromSeconds(15));

                foreach (var server in Servers)
                {
                    await WaitForValueAsync(async () =>
                    {
                    documentDatabase = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(database);
                    return documentDatabase.IndexStore.GetIndex(indexName).Status;
                    }, IndexRunningStatus.Running);
                    Assert.Equal(IndexState.Normal, documentDatabase.IndexStore.GetIndex(indexName).State);
                    Assert.Equal(IndexRunningStatus.Running, documentDatabase.IndexStore.GetIndex(indexName).Status);
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
