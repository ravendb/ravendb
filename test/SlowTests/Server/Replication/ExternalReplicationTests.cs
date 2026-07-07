using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.ServerWide.Operations;
using Raven.Server;
using Raven.Server.Documents.Replication.Stats;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Replication
{
    public class ExternalReplicationTests : ReplicationTestBase
    {
        public ExternalReplicationTests(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Replication)]
        [RavenData(3000, DatabaseMode = RavenDatabaseMode.All)]
        public async Task ExternalReplicationShouldWorkWithSmallTimeoutStress(Options options, int timeout)
        {
            using (var store1 = GetDocumentStore(new Options(options)
            {
                ModifyDatabaseName = s => $"{s}_FooBar-1"
            }))
            using (var store2 = GetDocumentStore(new Options(options)
            {
                ModifyDatabaseName = s => $"{s}_FooBar-2"
            }))
            using (var store3 = GetDocumentStore(new Options(options)
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

        [RavenTheory(RavenTestCategory.Replication)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task DelayedExternalReplication(Options options)
        {
            using (var store1 = GetDocumentStore(options))
            using (var store2 = GetDocumentStore(options))
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

        [RavenFact(RavenTestCategory.Replication)]
        public async Task ExternalReplicationShouldNotBeCountedAsSiblingInWaitForReplicationWithMajority()
        {
            using (var store1 = GetDocumentStore())
            using (var store2 = GetDocumentStore())
            {
                await SetupReplicationAsync(store1, store2);

                using (var s1 = store1.OpenSession())
                {
                    s1.Store(new User(), "foo/bar");
                    s1.SaveChanges();
                }

                // once the document arrived, the external replication connection was established
                // and the destination Url was resolved to this server's own url
                Assert.True(WaitForDocument(store2, "foo/bar"));

                // any database record change will cause ReplicationLoader.HandleTopologyChange
                // to recompute the number of siblings, this time with the resolved external destination url
                await store1.Maintenance.SendAsync(new PutConnectionStringOperation<RavenConnectionString>(new RavenConnectionString
                {
                    Name = "dummy",
                    Database = "dummy",
                    TopologyDiscoveryUrls = store1.Urls
                }));

                var database = await GetDocumentDatabaseInstanceFor(store1);

                // the database group has a single node, so there are no siblings
                // and a majority write-assurance requires 0 replicas
                var minReplicas = WaitForValue(() => database.ReplicationLoader.GetMinNumberOfReplicas(), 0, timeout: 5000);
                Assert.Equal(0, minReplicas);
                Assert.Equal(0, database.ReplicationLoader.NumberOfSiblingsInInternalReplication);

                using (var s1 = store1.OpenSession())
                {
                    s1.Advanced.WaitForReplicationAfterSaveChanges(timeout: TimeSpan.FromSeconds(10), majority: true);
                    s1.Store(new User(), "foo/bar/2");
                    s1.SaveChanges();
                }
            }
        }

        [RavenTheory(RavenTestCategory.Replication | RavenTestCategory.Cluster)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task RavenDB_17187_CheckInternalShowsInStats(Options options)
        {
            var database = GetDatabaseName();
            var cluster = await CreateRaftCluster(3);
            await CreateDatabaseInClusterForMode(database, 3, cluster, options.DatabaseMode);

            using (var store1 = new DocumentStore
            {
                Urls = new[] { cluster.Nodes[0].WebUrl },
                Conventions = { DisableTopologyUpdates = true },
                Database = database,
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

                var db = await cluster.Nodes[0].ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(options.DatabaseMode == RavenDatabaseMode.Single
                    ? database
                    : await Sharding.GetShardDatabaseNameForDocAsync((DocumentStore)store1, "foo/bar/store1"));
                var collector = new LiveReplicationPerformanceCollector(db);
                var stats = await collector.Stats.DequeueAsync();
                var incomingCount = stats.Count(performanceStats => performanceStats is LiveReplicationPerformanceCollector.IncomingPerformanceStats perf &&
                                                                    perf.Type == LiveReplicationPerformanceCollector.ReplicationPerformanceType.IncomingInternal);
                var outgoingCount = stats.Count(performanceStats => performanceStats is LiveReplicationPerformanceCollector.OutgoingPerformanceStats perf &&
                                                                   perf.Type == LiveReplicationPerformanceCollector.ReplicationPerformanceType.OutgoingInternal);
                Assert.True(stats.Count == 4, GetFailureMessage());
                Assert.Equal(2, incomingCount);
                Assert.Equal(2, outgoingCount);

                void WaitForDocumentInAllStores(string documentId)
                {
                    Assert.True(WaitForDocument(store1, documentId));
                    Assert.True(WaitForDocument(store2, documentId));
                    Assert.True(WaitForDocument(store3, documentId));
                }

                string GetFailureMessage()
                {
                    var databaseMembers = store1.Maintenance.Server.Send(new GetDatabaseRecordOperation(db.Name)).Topology.Members;
                    var clusterMembers = cluster.Leader.ServerStore.GetClusterTopology().Members.Keys.ToList();

                    return $"Failure encountered for database '{db.Name}'.{Environment.NewLine}" +
                           $"Expected stats count: 4, but actual stats count: {stats.Count}.{Environment.NewLine}" +
                           $"Current stats:{Environment.NewLine}" +
                           $"- Incoming internal handlers count: {incomingCount}{Environment.NewLine}" +
                           $"- Outgoing internal handlers count: {outgoingCount}{Environment.NewLine}" +
                           $"Database member nodes: [{string.Join(", ", databaseMembers)}]{Environment.NewLine}" +
                           $"Cluster member nodes: [{string.Join(", ", clusterMembers)}]";
                }
            }
        }

        [RavenTheory(RavenTestCategory.Replication)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task RavenDB_17187_CheckExternalShowsInStats(Options options)
        {
            var database = GetDatabaseName();

            using (var sender = GetNewServer(new ServerCreationOptions { }))
            using (var receiver = GetNewServer(new ServerCreationOptions { }))
            using (var senderStore = GetDocumentStore(new Options(options)
            {
                Server = sender,
                CreateDatabase = true,
                ModifyDatabaseName = (s) => database
            }))
            using (var receiverStore = GetDocumentStore(new Options(options)
            {
                Server = receiver,
                CreateDatabase = true,
                ModifyDatabaseName = (s) => database
            }))
            {
                await SetupReplicationAsync(senderStore, receiverStore);

                using (var session = senderStore.OpenSession(database))
                {
                    session.Store(new User(), "foo/bar");
                    session.SaveChanges();
                }

                Assert.True(WaitForDocument(receiverStore, "foo/bar"));

                var collector = await GetPerformanceCollectorAsync(senderStore, sender);
                var stats = await collector.Stats.DequeueAsync();
                Assert.Equal(1, stats.Count);
                Assert.Equal(LiveReplicationPerformanceCollector.ReplicationPerformanceType.OutgoingExternal, ((LiveReplicationPerformanceCollector.OutgoingPerformanceStats)stats[0]).Type);

                var expectedStatsCount = options.DatabaseMode == RavenDatabaseMode.Single ? 1 : 3; // for sharding we have 3 incoming connections (one for each shard in source)

                List<LiveReplicationPerformanceCollector.IReplicationPerformanceStats> stats2 = null;
                var statsCount = await WaitForValueAsync(async () =>
                {
                    var collector2 = await GetPerformanceCollectorAsync(receiverStore, receiver);
                    stats2 = await collector2.Stats.DequeueAsync();
                    return stats2.Count;
                }, expectedStatsCount);

                Assert.NotNull(stats2);
                Assert.Equal(expectedStatsCount, statsCount);
                for (var i = 0; i < expectedStatsCount; i++)
                {
                    Assert.Equal(LiveReplicationPerformanceCollector.ReplicationPerformanceType.IncomingExternal, ((LiveReplicationPerformanceCollector.IncomingPerformanceStats)stats2[i]).Type);
                }

                async Task<LiveReplicationPerformanceCollector> GetPerformanceCollectorAsync(DocumentStore store, RavenServer server)
                {
                    var db = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(options.DatabaseMode == RavenDatabaseMode.Single
                        ? database
                        : await Sharding.GetShardDatabaseNameForDocAsync(store, "foo/bar"));
                    return new LiveReplicationPerformanceCollector(db);
                }
            }
        }

        [RavenTheory(RavenTestCategory.Replication)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task EditExternalReplication(Options options)
        {
            using (var store1 = GetDocumentStore(options))
            using (var store2 = GetDocumentStore(options))
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

        [RavenTheory(RavenTestCategory.Replication)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task CanChangeConnectionString(Options options)
        {
            using (var store1 = GetDocumentStore(options))
            using (var store2 = GetDocumentStore(options))
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

        [RavenTheory(RavenTestCategory.Replication)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task ExternalReplicationToNonExistingDatabase(Options options)
        {
            using (var store1 = GetDocumentStore(options))
            using (var store2 = GetDocumentStore(options))
            {
                var externalTask = new ExternalReplication(store2.Database + "test", $"Connection to {store2.Database} test");
                await AddWatcherToReplicationTopology(store1, externalTask);
            }
        }

        [RavenTheory(RavenTestCategory.Replication)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task ExternalReplicationToNonExistingNode(Options options)
        {
            using (var store1 = GetDocumentStore(options))
            {
                var externalTask = new ExternalReplication(store1.Database + "test", $"Connection to {store1.Database} test");
                await AddWatcherToReplicationTopology(store1, externalTask, new[] { "http://1.2.3.4:8080" });
            }
        }
    }
}
