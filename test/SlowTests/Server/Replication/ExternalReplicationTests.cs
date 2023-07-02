using System;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.ServerWide;
using Raven.Server;
using Raven.Server.Documents.Replication.Stats;
using Raven.Tests.Core.Utils.Entities;
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

        [RavenTheory(RavenTestCategory.Replication | RavenTestCategory.Cluster)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task RavenDB_17187_CheckInternalShowsInStats(Options options)
        {
            var database = GetDatabaseName();
            var cluster = await CreateRaftCluster(3);

            var modifyDatabaseRecord = options.ModifyDatabaseRecord;
            var record = new DatabaseRecord(database);
            modifyDatabaseRecord?.Invoke(record);
            await CreateDatabaseInCluster(record, 3, cluster.Leader.WebUrl);

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
                Assert.True(stats.Count == 4, $"Expected: 4, actual: {stats.Count}. Exiting stats: Incoming internal handlers count: {incomingCount}. Outgoing internal handles count: {outgoingCount}");
                Assert.Equal(2, incomingCount);
                Assert.Equal(2, outgoingCount);

                void WaitForDocumentInAllStores(string documentId)
                {
                    Assert.True(WaitForDocument(store1, documentId));
                    Assert.True(WaitForDocument(store2, documentId));
                    Assert.True(WaitForDocument(store3, documentId));
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
                ;
                var collector2 = await GetPerformanceCollectorAsync(receiverStore, receiver);
                var stats2 = await collector2.Stats.DequeueAsync();

                var expectedStatsCount = options.DatabaseMode == RavenDatabaseMode.Single ? 1 : 3; // for sharding we have 3 incoming connections (one for each shard in source)
                Assert.Equal(expectedStatsCount, stats2.Count);
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
