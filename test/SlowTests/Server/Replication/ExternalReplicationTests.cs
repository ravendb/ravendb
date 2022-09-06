using System;
using System.Linq;
using System.Threading.Tasks;
using FastTests.Server.Replication;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Http;
using Raven.Server.Documents.Commands.Replication;
using Raven.Server.Documents.Handlers.Processors.Replication;
using Raven.Server.Documents.Replication.Stats;
using Raven.Server.Utils;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Json;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Replication
{
    public class ExternalReplicationTests : ReplicationTestBase
    {
        public ExternalReplicationTests(ITestOutputHelper output) : base(output)
        {
        }

        [Theory]
        [InlineData(3000)]
        public async Task ExternalReplicationShouldWorkWithSmallTimeoutStress(int timeout)
        {
            using (var store1 = GetDocumentStore(new Options
            {
                ModifyDatabaseName = s => $"{s}_FooBar-1"
            }))
            using (var store2 = GetDocumentStore(new Options
            {
                ModifyDatabaseName = s => $"{s}_FooBar-2"
            }))
            using (var store3 = GetDocumentStore(new Options
            {
                ModifyDatabaseName = s => $"{s}_FooBar-3"
            }))
            {
                await SetupReplicationAsync(store1, store2, store3);

                using (var s1 = store1.OpenSession())
                {
                    s1.Store(new User(), "foo/bar");
                    s1.SaveChanges();
                }
                // SlowTests uses the regular old value of 3000mSec. But if called from StressTests - needs more timeout
                Assert.True(WaitForDocument(store2, "foo/bar", timeout), store2.Identifier);
                Assert.True(WaitForDocument(store3, "foo/bar", timeout), store3.Identifier);
            }
        }

        [Fact]
        public async Task DelayedExternalReplication()
        {
            using (var store1 = GetDocumentStore())
            using (var store2 = GetDocumentStore())
            {
                var delay = TimeSpan.FromSeconds(5);
                var externalTask = new ExternalReplication(store2.Database, "DelayedExternalReplication")
                {
                    DelayReplicationFor = delay
                };
                await AddWatcherToReplicationTopology(store1, externalTask);
                DateTime date;

                using (var s1 = store1.OpenSession())
                {
                    s1.Store(new User(), "foo/bar");
                    date = DateTime.UtcNow;
                    s1.SaveChanges();
                }

                Assert.True(WaitForDocument(store2, "foo/bar"));
                var elapsed = DateTime.UtcNow - date;
                Assert.True(elapsed >= delay, $" only {elapsed}/{delay} ticks elapsed");

            }
        }

        [Fact]
        public async Task RavenDB_17187_CheckInternalShowsInStats()
        {
            var database = GetDatabaseName();
            var cluster = await CreateRaftCluster(3);
            await CreateDatabaseInCluster(database, 3, cluster.Leader.WebUrl);

            using (var store1 = new DocumentStore
            {
                Urls = new[] { cluster.Nodes[0].WebUrl },
                Conventions = { DisableTopologyUpdates = true },
                Database = database
            }.Initialize())
            using (var store2 = new DocumentStore
            {
                Urls = new[] { cluster.Nodes[1].WebUrl },
                Conventions = { DisableTopologyUpdates = true },
                Database = database,
            }.Initialize())
            using (var store3 = new DocumentStore
            {
                Urls = new[] { cluster.Nodes[2].WebUrl },
                Conventions = { DisableTopologyUpdates = true },
                Database = database
            }.Initialize())
            {
                using (var s1 = store1.OpenSession(database))
                {
                    s1.Store(new User(), "foo/bar/store1");
                    s1.SaveChanges();
                }

                using (var s2 = store2.OpenSession(database))
                {
                    s2.Store(new User(), "foo/bar/store2");
                    s2.SaveChanges();
                }

                using (var s3 = store3.OpenSession(database))
                {
                    s3.Store(new User(), "foo/bar/store3");
                    s3.SaveChanges();
                }

                WaitForDocumentInAllStores("foo/bar/store1");
                WaitForDocumentInAllStores("foo/bar/store2");
                WaitForDocumentInAllStores("foo/bar/store3");

                var collector = new LiveReplicationPerformanceCollector(await cluster.Nodes[0].ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(database));
                var stats = await collector.Stats.DequeueAsync();
                Assert.Equal(4, stats.Count);
                Assert.Equal(2,
                    stats.Count(performanceStats => performanceStats is LiveReplicationPerformanceCollector.IncomingPerformanceStats perf &&
                                        perf.Type == LiveReplicationPerformanceCollector.ReplicationPerformanceType.IncomingInternal));
                Assert.Equal(2,
                    stats.Count(performanceStats => performanceStats is LiveReplicationPerformanceCollector.OutgoingPerformanceStats perf &&
                                                    perf.Type == LiveReplicationPerformanceCollector.ReplicationPerformanceType.OutgoingInternal));

                void WaitForDocumentInAllStores(string documentId)
                {
                    WaitForDocument(store1, documentId);
                    WaitForDocument(store2, documentId);
                    WaitForDocument(store3, documentId);
                }
            }
        }

        [Fact]
        public async Task RavenDB_17187_CheckExternalShowsInStats()
        {
            var database = GetDatabaseName();

            using (var sender = GetNewServer(new ServerCreationOptions { }))
            using (var receiver = GetNewServer(new ServerCreationOptions { }))
            using (var senderStore = GetDocumentStore(new Options
            {
                Server = sender,
                CreateDatabase = true,
                ModifyDatabaseName = (s) => database
            }))
            using (var receiverStore = GetDocumentStore(new Options
            {
                Server = receiver,
                CreateDatabase = true,
                ModifyDatabaseName = (s) => database
            }))
            {
                await SetupReplicationAsync(senderStore, receiverStore);
                await EnsureReplicatingAsync(senderStore, receiverStore);

                var collector = new LiveReplicationPerformanceCollector(await sender.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(database));
                var stats = await collector.Stats.DequeueAsync();
                Assert.Equal(1, stats.Count);
                Assert.Equal(LiveReplicationPerformanceCollector.ReplicationPerformanceType.OutgoingExternal, ((LiveReplicationPerformanceCollector.OutgoingPerformanceStats)stats[0]).Type);

                var collector2 = new LiveReplicationPerformanceCollector(await receiver.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(database));
                var stats2 = await collector2.Stats.DequeueAsync();
                Assert.Equal(1, stats2.Count);
                Assert.Equal(LiveReplicationPerformanceCollector.ReplicationPerformanceType.IncomingExternal, ((LiveReplicationPerformanceCollector.IncomingPerformanceStats)stats2[0]).Type);
            }
        }

        [Fact]
        public async Task EditExternalReplication()
        {
            using (var store1 = GetDocumentStore())
            using (var store2 = GetDocumentStore())
            {
                var delay = TimeSpan.FromSeconds(5);
                var externalTask = new ExternalReplication(store2.Database, "DelayedExternalReplication")
                {
                    DelayReplicationFor = delay
                };
                await AddWatcherToReplicationTopology(store1, externalTask);
                DateTime date;

                using (var s1 = store1.OpenSession())
                {
                    s1.Store(new User(), "foo/bar");
                    date = DateTime.UtcNow;
                    s1.SaveChanges();
                }

                Assert.True(WaitForDocument(store2, "foo/bar"));
                var elapsed = DateTime.UtcNow - date;
                Assert.True(elapsed >= delay, $" only {elapsed}/{delay} ticks elapsed");

                delay = TimeSpan.Zero;
                externalTask.DelayReplicationFor = delay;
                var op = new UpdateExternalReplicationOperation(externalTask);
                await store1.Maintenance.SendAsync(op);

                using (var s1 = store1.OpenSession())
                {
                    s1.Store(new User(), "foo/bar");
                    date = DateTime.UtcNow;
                    s1.SaveChanges();
                }

                Assert.True(WaitForDocument(store2, "foo/bar"));
                var elapsedTime = DateTime.UtcNow - date;
                Assert.True(elapsedTime >= delay && elapsedTime < elapsed, $" only {elapsed}/{delay} ticks elapsed");
            }
        }

        [Fact]
        public async Task CanChangeConnectionString()
        {
            using (var store1 = GetDocumentStore())
            using (var store2 = GetDocumentStore())
            {
                var externalReplication = new ExternalReplication
                {
                    ConnectionStringName = "ExReplication"
                };
                await store1.Maintenance.SendAsync(new PutConnectionStringOperation<RavenConnectionString>(new RavenConnectionString
                {
                    Name = externalReplication.ConnectionStringName,
                    Database = "NotExist",
                    TopologyDiscoveryUrls = store1.Urls
                }));
                await store1.Maintenance.SendAsync(new UpdateExternalReplicationOperation(externalReplication));

                using (var s1 = store1.OpenSession())
                {
                    s1.Store(new User(), "foo/bar");
                    s1.SaveChanges();
                }

                Assert.False(WaitForDocument(store2, "foo/bar", timeout: 5_000));

                await store1.Maintenance.SendAsync(new PutConnectionStringOperation<RavenConnectionString>(new RavenConnectionString
                {
                    Name = externalReplication.ConnectionStringName,
                    Database = store2.Database,
                    TopologyDiscoveryUrls = store1.Urls
                }));

                Assert.True(WaitForDocument(store2, "foo/bar", timeout: 5_000));

                await store1.Maintenance.SendAsync(new PutConnectionStringOperation<RavenConnectionString>(new RavenConnectionString
                {
                    Name = externalReplication.ConnectionStringName,
                    Database = "NotExist",
                    TopologyDiscoveryUrls = store1.Urls
                }));

                using (var s1 = store1.OpenSession())
                {
                    s1.Store(new User(), "foo/bar/2");
                    s1.SaveChanges();
                }
                Assert.False(WaitForDocument(store2, "foo/bar/2", timeout: 5_000));
            }
        }


        [Fact]
        public async Task ExternalReplicationToNonExistingDatabase()
        {
            using (var store1 = GetDocumentStore())
            using (var store2 = GetDocumentStore())
            {
                var externalTask = new ExternalReplication(store2.Database + "test", $"Connection to {store2.Database} test");
                await AddWatcherToReplicationTopology(store1, externalTask);
            }
        }

        [Fact]
        public async Task ExternalReplicationToNonExistingNode()
        {
            using (var store1 = GetDocumentStore())
            {
                var externalTask = new ExternalReplication(store1.Database + "test", $"Connection to {store1.Database} test");
                await AddWatcherToReplicationTopology(store1, externalTask, new[] { "http://1.2.3.4:8080" });
            }
        }

        private const DatabaseItemType AllTypes = DatabaseItemType.Documents | DatabaseItemType.RevisionDocuments | DatabaseItemType.Tombstones |
                                                  DatabaseItemType.Conflicts | DatabaseItemType.Attachments | DatabaseItemType.CounterGroups |
                                                  DatabaseItemType.TimeSeries;

        [Fact]
        public async Task ExternalReplicationFromNonShardedToShardedShouldWork()
        {
            using (var store1 = GetDocumentStore())
            using (var store2 = Sharding.GetDocumentStore())
            {
                await SetupReplicationAsync(store1, store2);

                using (var s1 = store1.OpenSession())
                {
                    for (int i = 0; i < 50; i++)
                    {
                        s1.Store(new User(), $"foo/bar/{i}");
                    }
                    
                    s1.SaveChanges();
                }

                for (int i = 0; i < 50; i++)
                {
                    var id = $"foo/bar/{i}";
                    Assert.True(WaitForDocument<User>(store2, id, predicate: null, timeout: 30_000));
                }
            }
        }

        [Fact]
        public async Task ExternalReplicationFromNonShardedToShardedShouldWork2()
        {
            using (var store1 = GetDocumentStore(new Options
                   {
                       ModifyDatabaseName = s => $"NonSharded_{s}"
                   }))
            using (var store2 = Sharding.GetDocumentStore(new Options
                   {
                       ModifyDatabaseName = s => $"Sharded_{s}"
                   }))
            {
                await SetupReplicationAsync(store1, store2);

                await store1.Maintenance.SendAsync(new CreateSampleDataOperation(AllTypes));

                await EnsureReplicatingAsync(store1, store2);

                WaitForUserToContinueTheTest(store2);
            }
        }

        [Fact]
        public async Task ExternalReplicationFromShardedToShardedShouldWork()
        {
            using (var store1 = Sharding.GetDocumentStore(new Options
                   {
                       ModifyDatabaseName = s => $"Source_{s}"
                   }))
            using (var store2 = Sharding.GetDocumentStore(new Options
                   {
                       ModifyDatabaseName = s => $"Destination_{s}"
                   }))
            {
                await SetupReplicationAsync(store1, store2);

                using (var s1 = store1.OpenSession())
                {
                    for (int i = 0; i < 50; i++)
                    {
                        s1.Store(new User(), $"foo/bar/{i}");
                    }

                    s1.SaveChanges();
                }

                for (int i = 0; i < 50; i++)
                {
                    var id = $"foo/bar/{i}";
                    Assert.True(WaitForDocument<User>(store2, id, predicate: null, timeout: 30_000));
                }
            }
        }

        [Fact]
        public async Task ExternalReplicationFromShardedToShardedShouldWork2()
        {
            using (var store1 = Sharding.GetDocumentStore(new Options
                   {
                       ModifyDatabaseName = s => $"NonSharded_{s}"
                   }))
            using (var store2 = Sharding.GetDocumentStore(new Options
                   {
                       ModifyDatabaseName = s => $"Sharded_{s}"
                   }))
            {
                await SetupReplicationAsync(store1, store2);

                await store1.Maintenance.SendAsync(new CreateSampleDataOperation(AllTypes));
                WaitForUserToContinueTheTest(store2);

                await EnsureReplicatingAsync(store1, store2);
                await EnsureReplicatingAsync(store1, store2);
                await EnsureReplicatingAsync(store1, store2);
                await EnsureReplicatingAsync(store1, store2);
                await EnsureReplicatingAsync(store1, store2);
                await EnsureReplicatingAsync(store1, store2);
                
                WaitForUserToContinueTheTest(store2);
            }
        }

        [Fact]
        public async Task EnsureNoReplicationLoopInExternalReplicationBetweenTwoShardedDBs()
        {
            using (var store1 = Sharding.GetDocumentStore())
            using (var store2 = Sharding.GetDocumentStore())
            {
                using (var s1 = store1.OpenSession())
                {
                    for (int i = 0; i < 50; i++)
                    {
                        s1.Store(new User(), $"foo/bar/{i}");
                    }

                    s1.SaveChanges();
                }

                using (var s2 = store2.OpenSession())
                {
                    for (int i = 0; i < 50; i++)
                    {
                        s2.Store(new User(), $"users/{i}");
                    }

                    s2.SaveChanges();
                }

                await SetupReplicationAsync(store1, store2);
                await SetupReplicationAsync(store2, store1);

                await EnsureReplicatingAsync(store1, store2);
                await EnsureReplicatingAsync(store2, store1);

                await ShardingCluster.EnsureNoReplicationLoopForSharding(Server, store1.Database);
                await ShardingCluster.EnsureNoReplicationLoopForSharding(Server, store2.Database);

                for (int i = 0; i < 50; i++)
                {
                    var id = $"foo/bar/{i}";
                    Assert.True(WaitForDocument<User>(store2, id, predicate: null, timeout: 30_000));
                }

                for (int i = 0; i < 50; i++)
                {
                    var id = $"users/{i}";
                    Assert.True(WaitForDocument<User>(store1, id, predicate: null, timeout: 30_000));
                }
            }
        }

        [Fact]
        public async Task EnsureNoReplicationLoopInExternalReplicationBetweenTwoShardedDBs2()
        {
            using (var store1 = Sharding.GetDocumentStore())
            using (var store2 = Sharding.GetDocumentStore())
            {
                using (var s1 = store1.OpenSession())
                {
                    s1.Store(new User() { Name = "Shiran" }, "users/1");
                    s1.SaveChanges();
                }

                using (var s2 = store2.OpenSession())
                {
                    s2.Store(new User() { Name = "Queeni" }, "users/1");
                    s2.SaveChanges();
                }

                await SetupReplicationAsync(store1, store2);
                await EnsureReplicatingAsync(store1, store2);

                await SetupReplicationAsync(store2, store1);
                await EnsureReplicatingAsync(store2, store1);

                await ShardingCluster.EnsureNoReplicationLoopForSharding(Server, store1.Database);
                await ShardingCluster.EnsureNoReplicationLoopForSharding(Server, store2.Database);

                Assert.True(WaitForDocument<User>(store1, "users/1", predicate: u => u.Name == "Queeni", timeout: 30_000));
                Assert.True(WaitForDocument<User>(store2, "users/1", predicate: u => u.Name == "Queeni", timeout: 30_000));
            }
        }

        [Fact]
        public async Task ExternalReplicationFromShardedToNonShardedShouldWork()
        {
            using (var store1 = Sharding.GetDocumentStore())
            using (var store2 = GetDocumentStore())
            {
                await SetupReplicationAsync(store1, store2);

                using (var s1 = store1.OpenSession())
                {
                    for (int i = 0; i < 50; i++)
                    {
                        s1.Store(new User(), $"foo/bar/{i}");
                    }

                    s1.SaveChanges();
                }

                for (int i = 0; i < 50; i++)
                {
                    var id = $"foo/bar/{i}";
                    Assert.True(WaitForDocument<User>(store2, id, predicate: null, timeout: 30_000));
                }
            }
        }

        [Fact]
        public async Task ExternalReplicationFromShardedToNonShardedShouldWork2()
        {
            using (var store1 = Sharding.GetDocumentStore(new Options
                   {
                       ModifyDatabaseName = s => $"Sharded_{s}"
                   }))
            using (var store2 = GetDocumentStore())
            {
                await SetupReplicationAsync(store1, store2);

                await store1.Maintenance.SendAsync(new CreateSampleDataOperation(AllTypes));

                await EnsureReplicatingAsync(store1, store2);
                
                WaitForUserToContinueTheTest(store2);
            }
        }

        [Fact]
        public async Task GetReplicationActiveConnectionsShouldWork()
        {
            using (var store1 = Sharding.GetDocumentStore())
            using (var store2 = Sharding.GetDocumentStore())
            {
                await SetupReplicationAsync(store1, store2);

                using (var s1 = store1.OpenSession())
                {
                    for (int i = 0; i < 50; i++)
                    {
                        s1.Store(new User(), $"foo/bar/{i}");
                    }

                    s1.SaveChanges();
                }

                for (int i = 0; i < 50; i++)
                {
                    var id = $"foo/bar/{i}";
                    Assert.True(WaitForDocument<User>(store2, id, predicate: null, timeout: 30_000));
                }

                var dbs = Server.ServerStore.DatabasesLandlord.TryGetOrCreateShardedResourcesStore(store2.Database);
                foreach (var task in dbs)
                {
                    var db = await task;
                    var shardNumber = ShardHelper.GetShardNumber(db.Name);
                    var replicationActiveConnections = await store2.Maintenance.ForShard(shardNumber).SendAsync(new GetReplicationActiveConnectionsInfoOperation());
                    Assert.NotNull(replicationActiveConnections.IncomingConnections);
                    Assert.Equal(3, replicationActiveConnections.IncomingConnections.Count);
                    Assert.Empty(replicationActiveConnections.OutgoingConnections);
                }
            }
        }

        public class GetReplicationActiveConnectionsInfoOperation : IMaintenanceOperation<ReplicationActiveConnectionsPreview>
        {
            public RavenCommand<ReplicationActiveConnectionsPreview> GetCommand(DocumentConventions conventions, JsonOperationContext context)
            {
                return new GetReplicationActiveConnectionsInfoCommand();
            }
        }
    }
}
