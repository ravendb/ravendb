using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using FastTests.Server.Replication;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Config;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace StressTests.Server.Replication
{
    public class ExternalReplicationStressTests : ReplicationTestBase
    {
        public ExternalReplicationStressTests(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task TwoWayExternalReplicationShouldNotLoadIdleDatabase()
        {
            using (var server = GetNewServer(new ServerCreationOptions
            {
                CustomSettings = new Dictionary<string, string>
                {
                    [RavenConfiguration.GetKey(x => x.Databases.MaxIdleTime)] = "10",
                    [RavenConfiguration.GetKey(x => x.Databases.FrequencyToCheckForIdle)] = "3",
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
                var externalTask1 = new ExternalReplication(store2.Database, "MyConnectionString1")
                {
                    Name = "MyExternalReplication1"
                };

                var externalTask2 = new ExternalReplication(store1.Database, "MyConnectionString2")
                {
                    Name = "MyExternalReplication2"
                };
                await AddWatcherToReplicationTopology(store1, externalTask1);
                await AddWatcherToReplicationTopology(store2, externalTask2);

                Assert.True(server.ServerStore.DatabasesLandlord.LastRecentlyUsed.TryGetValue(store1.Database, out _));
                Assert.True(server.ServerStore.DatabasesLandlord.LastRecentlyUsed.TryGetValue(store2.Database, out _));

                var now = DateTime.Now;
                var nextNow = now + TimeSpan.FromSeconds(60);
                while (now < nextNow && server.ServerStore.IdleDatabases.Count < 2)
                {
                    await Task.Delay(3000);
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
                    await Task.Delay(3000);
                    replicatedDocs = store2.Maintenance.Send(new GetStatisticsOperation()).CountOfDocuments;
                    count++;
                }
                Assert.Equal(docs, replicatedDocs);

                count = 0;
                nextNow = DateTime.Now + TimeSpan.FromMinutes(5);
                while (server.ServerStore.IdleDatabases.Count == 0 && now < nextNow)
                {
                    await Task.Delay(500);
                    if (count % 10 == 0)
                        store1.Maintenance.Send(new GetStatisticsOperation());

                    now = DateTime.Now;
                    count++;
                }
                Assert.Equal(1, server.ServerStore.IdleDatabases.Count);

                nextNow = DateTime.Now + TimeSpan.FromSeconds(15);
                while (now < nextNow)
                {
                    await Task.Delay(2000);
                    store1.Maintenance.Send(new GetStatisticsOperation());
                    Assert.Equal(1, server.ServerStore.IdleDatabases.Count);
                    now = DateTime.Now;
                }

                nextNow = DateTime.Now + TimeSpan.FromMinutes(10);
                while (now < nextNow && server.ServerStore.IdleDatabases.Count < 2)
                {
                    await Task.Delay(3000);
                    now = DateTime.Now;
                }
                Assert.Equal(2, server.ServerStore.IdleDatabases.Count);

                using (var s = store2.OpenSession())
                {
                    s.Advanced.RawQuery<dynamic>("from @all_docs")
                        .ToList();
                }
                Assert.Equal(1, server.ServerStore.IdleDatabases.Count);

                var operation = await store2
                    .Operations
                    .ForDatabase(store2.Database)
                    .SendAsync(new PatchByQueryOperation("from Companies update { this.Name = this.Name + '_patched'; }"));

                await operation.WaitForCompletionAsync();

                nextNow = DateTime.Now + TimeSpan.FromMinutes(2);
                while (now < nextNow && server.ServerStore.IdleDatabases.Count > 0)
                {
                    await Task.Delay(5000);
                    now = DateTime.Now;
                }
                Assert.Equal(0, server.ServerStore.IdleDatabases.Count);

                nextNow = DateTime.Now + TimeSpan.FromMinutes(10);
                while (server.ServerStore.IdleDatabases.Count == 0 && now < nextNow)
                {
                    await Task.Delay(500);
                    if (count % 10 == 0)
                        store2.Maintenance.Send(new GetStatisticsOperation());

                    now = DateTime.Now;
                    count++;
                }
                Assert.Equal(1, server.ServerStore.IdleDatabases.Count);

                nextNow = DateTime.Now + TimeSpan.FromSeconds(15);
                while (now < nextNow)
                {
                    await Task.Delay(2000);
                    store2.Maintenance.Send(new GetStatisticsOperation());
                    Assert.Equal(1, server.ServerStore.IdleDatabases.Count);
                    now = DateTime.Now;
                }
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
                    await Task.Delay(3000);
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
                    await Task.Delay(3000);
                    replicatedDocs = store2.Maintenance.Send(new GetStatisticsOperation()).CountOfDocuments;
                    count++;
                }
                Assert.Equal(docs, replicatedDocs);

                count = 0;
                nextNow = DateTime.Now + TimeSpan.FromMinutes(5);
                while (server.ServerStore.IdleDatabases.Count == 0 && now < nextNow)
                {
                    await Task.Delay(500);
                    if (count % 10 == 0)
                        store1.Maintenance.Send(new GetStatisticsOperation());

                    now = DateTime.Now;
                    count++;
                }
                Assert.Equal(1, server.ServerStore.IdleDatabases.Count);

                nextNow = DateTime.Now + TimeSpan.FromSeconds(15);
                while (now < nextNow)
                {
                    await Task.Delay(2000);
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
                await Task.Delay(3000);
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
                await Task.Delay(3000);
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
                    await Task.Delay(3000);
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
