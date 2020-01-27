using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FastTests.Server.Replication;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Config;
using Raven.Server.ServerWide.Commands;
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
                await AddWatcherToReplicationTopology(store1, externalTask, new []{"http://1.2.3.4:8080"});
            }
        }

        [Fact]
        public async Task ExternalReplicationShouldNotLoadIdleDatabase()
        {
            using (var server = GetNewServer(new ServerCreationOptions
            {
                CustomSettings = new Dictionary<string, string>
                {
                    [RavenConfiguration.GetKey(x => x.Databases.MaxIdleTime)] = "10",
                    [RavenConfiguration.GetKey(x => x.Databases.FrequencyToCheckForIdle)] = "3",
                    [RavenConfiguration.GetKey(x => x.Replication.RetryMaxTimeout)] = "1",
                    [RavenConfiguration.GetKey(x => x.Core.RunInMemory)] = "false"

                }
            }))
            using (var store1 = GetDocumentStore(new Options
            {
                Server = server,
                RunInMemory = false

            }))
            using (var store2 = GetDocumentStore(new Options
            {
                Server = server,
                RunInMemory = false
            }))
            {
                // lowercase for test 
                var externalTask = new ExternalReplication(store2.Database.ToLowerInvariant(), "MyConnectionString")
                {
                    Name = "MyExternalReplication"
                };

                await AddWatcherToReplicationTopology(store1, externalTask);

                Assert.True(server.ServerStore.DatabasesLandlord.LastRecentlyUsed.TryGetValue(store1.Database, out _));
                Assert.True(server.ServerStore.DatabasesLandlord.LastRecentlyUsed.TryGetValue(store2.Database, out _));

                var now = DateTime.Now;
                var nextNow = now + TimeSpan.FromSeconds(60);
                while (now < nextNow && server.ServerStore.IdleDatabases.Count < 2)
                {
                    Thread.Sleep(3000);
                    now = DateTime.Now;
                }

                Assert.Equal(2, server.ServerStore.IdleDatabases.Count);

                await store1.Maintenance.SendAsync(new CreateSampleDataOperation());
                WaitForIndexing(store1);

                var count = 0;
                var docs = store1.Maintenance.Send(new GetStatisticsOperation()).CountOfDocuments;
                var replicatedDocs = store2.Maintenance.Send(new GetStatisticsOperation()).CountOfDocuments;
                while (docs != replicatedDocs && count < 20)
                {
                    Thread.Sleep(3000);
                    replicatedDocs = store2.Maintenance.Send(new GetStatisticsOperation()).CountOfDocuments;
                    count++;
                }
                Assert.Equal(docs, replicatedDocs);

                count = 0;
                nextNow = DateTime.Now + TimeSpan.FromMinutes(5);
                while (server.ServerStore.IdleDatabases.Count == 0 && now < nextNow)
                {
                    Thread.Sleep(500);
                    if (count % 10 == 0)
                        store1.Maintenance.Send(new GetStatisticsOperation());

                    now = DateTime.Now;
                    count++;
                }
                Assert.Equal(1, server.ServerStore.IdleDatabases.Count);

                nextNow = DateTime.Now + TimeSpan.FromSeconds(15);
                while (now < nextNow)
                {
                    Thread.Sleep(2000);
                    store1.Maintenance.Send(new GetStatisticsOperation());
                    Assert.Equal(1, server.ServerStore.IdleDatabases.Count);
                    now = DateTime.Now;
                }
            }
        }

        [Fact]
        public async Task CanIdleDatabaseInCluster()
        {
            const int clusterSize = 3;
            var databaseName = GetDatabaseName();
            var leader = await CreateRaftClusterAndGetLeader(3, false, customSettings: new Dictionary<string, string>()
            {
                [RavenConfiguration.GetKey(x => x.Cluster.MoveToRehabGraceTime)] = "1",
                [RavenConfiguration.GetKey(x => x.Cluster.AddReplicaTimeout)] = "1",
                [RavenConfiguration.GetKey(x => x.Cluster.ElectionTimeout)] = "300",
                [RavenConfiguration.GetKey(x => x.Cluster.StabilizationTime)] = "1",
                [RavenConfiguration.GetKey(x => x.Databases.MaxIdleTime)] = "10",
                [RavenConfiguration.GetKey(x => x.Replication.RetryMaxTimeout)] = "1",
                [RavenConfiguration.GetKey(x => x.Databases.FrequencyToCheckForIdle)] = "3",
                [RavenConfiguration.GetKey(x => x.Core.RunInMemory)] = "false"
            });

            DatabasePutResult databaseResult;
            using (var store = new DocumentStore
            {
                Urls = new[] { leader.WebUrl },
                Database = databaseName
            }.Initialize())
            {
                var doc = new DatabaseRecord(databaseName);
                databaseResult = await store.Maintenance.Server.SendAsync(new CreateDatabaseOperation(doc, clusterSize));
            }
            Assert.Equal(clusterSize, databaseResult.Topology.AllNodes.Count());

            foreach (var server in Servers)
            {
                await server.ServerStore.Cluster.WaitForIndexNotification(databaseResult.RaftCommandIndex);
            }
            foreach (var server in Servers.Where(s => databaseResult.NodesAddedTo.Any(n => n == s.WebUrl)))
            {
                await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(databaseName);
            }

            var now = DateTime.Now;
            var nextNow = now + TimeSpan.FromSeconds(300);
            while (now < nextNow && GetIdleCount() < clusterSize)
            {
                Thread.Sleep(3000);
                now = DateTime.Now;
            }

            foreach (var server in Servers)
            {
                Assert.Equal(1, server.ServerStore.IdleDatabases.Count);
                Assert.True(server.ServerStore.IdleDatabases.TryGetValue(databaseName, out var dictionary));

                // new incoming replications not saved in IdleDatabases
                Assert.Equal(0, dictionary.Count);
            }

            var rnd = new Random();
            var index = rnd.Next(0, Servers.Count - 1);
            using (var store = new DocumentStore
            {
                Urls = new[] { Servers[index].WebUrl },
                Database = databaseName
            }.Initialize())
            {
                await store.Maintenance.SendAsync(new GetStatisticsOperation());
            }

            Assert.Equal(2, GetIdleCount());

            using (var store = new DocumentStore
            {
                Urls = new[] { Servers[index].WebUrl },
                Database = databaseName
            }.Initialize())
            {
                using (var s = store.OpenAsyncSession())
                {
                    await s.StoreAsync(new User()
                    {
                        Name = "Egor"
                    }, "foo/bar");

                    await s.SaveChangesAsync();
                }
            }

            nextNow = DateTime.Now + TimeSpan.FromSeconds(300);
            while (now < nextNow && GetIdleCount() > 0)
            {
                Thread.Sleep(3000);
                now = DateTime.Now;
            }

            Assert.Equal(0, GetIdleCount());

            foreach (var server in Servers)
            {
                using (var store = new DocumentStore
                {
                    Urls = new[] { server.WebUrl },
                    Database = databaseName
                }.Initialize())
                {
                    var docs = (await store.Maintenance.SendAsync(new GetStatisticsOperation())).CountOfDocuments;
                    Assert.Equal(1, docs);
                }
            }

            index = rnd.Next(0, Servers.Count - 1);
            nextNow = DateTime.Now + TimeSpan.FromSeconds(300);

            using (var store = new DocumentStore
            {
                Urls = new[] { Servers[index].WebUrl },
                Database = databaseName
            }.Initialize())
            {
                while (now < nextNow && GetIdleCount() < 2)
                {
                    Thread.Sleep(3000);
                    await store.Maintenance.SendAsync(new GetStatisticsOperation());
                    now = DateTime.Now;
                }
            }

            Assert.Equal(2, GetIdleCount());
        }

        private int GetIdleCount()
        {
            int idleCount = 0;
            foreach (var server in Servers)
            {
                if (server.ServerStore.IdleDatabases.Count == 1)
                    idleCount++;
            }

            return idleCount;
        }
    }
}
