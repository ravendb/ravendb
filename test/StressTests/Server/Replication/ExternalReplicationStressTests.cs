using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Replication;
using Raven.Server;
using Raven.Server.Config;
using Raven.Server.ServerWide;
using Raven.Tests.Core.Utils.Entities;
using StressTests.Issues;
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

        [RavenFact(RavenTestCategory.Core)]
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

                Assert.True(WaitForValue(() =>
                        server.ServerStore.DatabaseIdleManager.GetActivityState(store1.Database) is DatabaseIdleManager.DatabaseActivityState.Idle &&
                        server.ServerStore.DatabaseIdleManager.GetActivityState(store2.Database) is DatabaseIdleManager.DatabaseActivityState.Idle,
                    expectedVal: true,
                    timeout: (int) TimeSpan.FromSeconds(60).TotalMilliseconds,
                    interval: (int) TimeSpan.FromSeconds(3).TotalMilliseconds));

                await store1.Maintenance.SendAsync(new CreateSampleDataOperation());
                Indexes.WaitForIndexing(store1);

                var docs = store1.Maintenance.Send(new GetStatisticsOperation()).CountOfDocuments;

                Assert.True(WaitForValue(() =>
                    {
                        var replicatedDocs = store2.Maintenance.Send(new GetStatisticsOperation()).CountOfDocuments;
                        return docs == replicatedDocs;
                    },
                    expectedVal: true,
                    timeout: (int)TimeSpan.FromSeconds(60).TotalMilliseconds,
                    interval: (int)TimeSpan.FromSeconds(3).TotalMilliseconds));

                var count = 0;
                Assert.True(WaitForValue(() =>
                    {
                        if (count % 10 == 0)
                            store1.Maintenance.Send(new GetStatisticsOperation());

                        count++;

                        return server.ServerStore.DatabaseIdleManager.GetActivityState(store1.Database) is DatabaseIdleManager.DatabaseActivityState.Active &&
                               server.ServerStore.DatabaseIdleManager.GetActivityState(store2.Database) is DatabaseIdleManager.DatabaseActivityState.Idle;
                    },
                    expectedVal: true,
                    timeout: (int)TimeSpan.FromMinutes(5).TotalMilliseconds,
                    interval: 500));

                Assert.True(WaitForValue(() =>
                    {
                        store1.Maintenance.Send(new GetStatisticsOperation());
                        Assert.True(server.ServerStore.DatabaseIdleManager.GetActivityState(store2.Database) is DatabaseIdleManager.DatabaseActivityState.Idle);

                        return server.ServerStore.DatabaseIdleManager.GetActivityState(store1.Database) is DatabaseIdleManager.DatabaseActivityState.Active;
                    },
                    expectedVal: true,
                    timeout: (int)TimeSpan.FromSeconds(15).TotalMilliseconds,
                    interval: (int)TimeSpan.FromSeconds(2).TotalMilliseconds));

                Assert.True(WaitForValue(() =>
                        server.ServerStore.DatabaseIdleManager.GetActivityState(store1.Database) is DatabaseIdleManager.DatabaseActivityState.Idle &&
                        server.ServerStore.DatabaseIdleManager.GetActivityState(store2.Database) is DatabaseIdleManager.DatabaseActivityState.Idle,
                    expectedVal: true,
                    timeout: (int) TimeSpan.FromMinutes(10).TotalMilliseconds,
                    interval: (int) TimeSpan.FromSeconds(3).TotalMilliseconds));

                using (var s = store2.OpenSession())
                {
                    s.Advanced.RawQuery<dynamic>("from @all_docs")
                        .ToList();
                }
                Assert.Equal(DatabaseIdleManager.DatabaseActivityState.Active, server.ServerStore.DatabaseIdleManager.GetActivityState(store2.Database));

                var operation = await store2
                    .Operations
                    .ForDatabase(store2.Database)
                    .SendAsync(new PatchByQueryOperation("from Companies update { this.Name = this.Name + '_patched'; }"));

                await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(5));

                Assert.True(WaitForValue(() =>
                        server.ServerStore.DatabaseIdleManager.GetActivityState(store1.Database) is DatabaseIdleManager.DatabaseActivityState.Active &&
                        server.ServerStore.DatabaseIdleManager.GetActivityState(store2.Database) is DatabaseIdleManager.DatabaseActivityState.Active,
                    expectedVal: true,
                    timeout: (int) TimeSpan.FromMinutes(2).TotalMilliseconds,
                    interval: (int) TimeSpan.FromSeconds(5).TotalMilliseconds));

                count = 0;
                Assert.True(WaitForValue(() =>
                    {
                        if (count % 10 == 0)
                            store2.Maintenance.Send(new GetStatisticsOperation());

                        count++;

                        return server.ServerStore.DatabaseIdleManager.GetActivityState(store1.Database) is DatabaseIdleManager.DatabaseActivityState.Idle &&
                               server.ServerStore.DatabaseIdleManager.GetActivityState(store2.Database) is DatabaseIdleManager.DatabaseActivityState.Active;
                    },
                    expectedVal: true,
                    timeout: (int)TimeSpan.FromMinutes(10).TotalMilliseconds,
                    interval: 500));

                Assert.True(WaitForValue(() =>
                    {
                        store2.Maintenance.Send(new GetStatisticsOperation());
                        Assert.True(server.ServerStore.DatabaseIdleManager.GetActivityState(store1.Database) is DatabaseIdleManager.DatabaseActivityState.Idle);

                        return server.ServerStore.DatabaseIdleManager.GetActivityState(store2.Database) is DatabaseIdleManager.DatabaseActivityState.Active;
                    },
                    expectedVal: true,
                    timeout: (int)TimeSpan.FromSeconds(15).TotalMilliseconds,
                    interval: (int)TimeSpan.FromSeconds(2).TotalMilliseconds));
            }
        }

        [RavenFact(RavenTestCategory.Core)]
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

                Assert.True(WaitForValue(() =>
                        server.ServerStore.DatabaseIdleManager.GetActivityState(store1.Database) is DatabaseIdleManager.DatabaseActivityState.Idle &&
                        server.ServerStore.DatabaseIdleManager.GetActivityState(store2.Database) is DatabaseIdleManager.DatabaseActivityState.Idle,
                    expectedVal: true,
                    timeout: (int) TimeSpan.FromSeconds(60).TotalMilliseconds,
                    interval: (int) TimeSpan.FromSeconds(3).TotalMilliseconds));

                await store1.Maintenance.SendAsync(new CreateSampleDataOperation());
                Indexes.WaitForIndexing(store1);

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
                Assert.True(WaitForValue(() =>
                    {
                        if (count % 10 == 0)
                            store1.Maintenance.Send(new GetStatisticsOperation());

                        count++;

                        return server.ServerStore.DatabaseIdleManager.GetActivityState(store1.Database) is DatabaseIdleManager.DatabaseActivityState.Active &&
                               server.ServerStore.DatabaseIdleManager.GetActivityState(store2.Database) is DatabaseIdleManager.DatabaseActivityState.Idle;
                    },
                    expectedVal: true,
                    timeout: (int)TimeSpan.FromMinutes(5).TotalMilliseconds,
                    interval: 500));

                Assert.True(WaitForValue(() =>
                    {
                        store1.Maintenance.Send(new GetStatisticsOperation());
                        Assert.True(server.ServerStore.DatabaseIdleManager.GetActivityState(store2.Database) is DatabaseIdleManager.DatabaseActivityState.Idle);

                        return server.ServerStore.DatabaseIdleManager.GetActivityState(store1.Database) is DatabaseIdleManager.DatabaseActivityState.Active;
                    },
                    expectedVal: true,
                    timeout: (int)TimeSpan.FromSeconds(15).TotalMilliseconds,
                    interval: (int)TimeSpan.FromSeconds(2).TotalMilliseconds));
            }
        }

        private List<RavenServer> _nodes;

        [RavenFact(RavenTestCategory.Core)]
        public async Task CanIdleDatabaseInCluster()
        {
            const int clusterSize = 3;
            var databaseName = GetDatabaseName();

            var cluster = await CreateRaftCluster(numberOfNodes: clusterSize, shouldRunInMemory: false, customSettings: new Dictionary<string, string>()
            {
                [RavenConfiguration.GetKey(x => x.Cluster.MoveToRehabGraceTime)] = "10",
                [RavenConfiguration.GetKey(x => x.Cluster.AddReplicaTimeout)] = "1",
                [RavenConfiguration.GetKey(x => x.Cluster.ElectionTimeout)] = "300",
                [RavenConfiguration.GetKey(x => x.Cluster.StabilizationTime)] = "1",
                [RavenConfiguration.GetKey(x => x.Databases.MaxIdleTime)] = "10",
                [RavenConfiguration.GetKey(x => x.Databases.FrequencyToCheckForIdle)] = "3"
            });

            _nodes = cluster.Nodes;
            var wakeUpReasons = new Dictionary<string, List<string>>();
            try
            {
                foreach (var server in _nodes)
                {
                    wakeUpReasons.Add(server.ServerStore.NodeTag, new List<string>());
                    server.ServerStore.DatabaseIdleManager.ForTestingPurposesOnly().SkipShouldContinueDisposeCheck = true;
                    server.ServerStore.DatabasesLandlord.ForTestingPurposesOnly().AfterDatabaseCreation = tuple =>
                    {
                        var list = wakeUpReasons[tuple.Database.ServerStore.NodeTag];
                        list.Add(tuple.caller);
                        wakeUpReasons[tuple.Database.ServerStore.NodeTag] = list;
                    };
                }

                using (var store = GetDocumentStore(new Options
                {
                    ModifyDatabaseName = s => databaseName,
                    ReplicationFactor = clusterSize,
                    Server = cluster.Leader,
                    RunInMemory = false
                }))
                {
                    var count = RavenDB_13987.WaitForCount(TimeSpan.FromSeconds(300), clusterSize, () => GetIdleCount(store.Database));
                    Assert.True(clusterSize == count, string.Join(Environment.NewLine, wakeUpReasons.Select(x => string.Join(": ", x.Key, string.Join(", ", x.Value)))));

                    foreach (var server in _nodes)
                    {
                        Assert.True(server.ServerStore.DatabaseIdleManager.TryGetIdleInfo(databaseName, out var idleDatabaseInfo));

                        // new incoming replications not saved in IdleDatabases
                        Assert.Equal(0, idleDatabaseInfo.ReplicationInfo.Count);
                    }

                    var rnd = new Random();
                    var index = rnd.Next(0, clusterSize);
                    using (var store2 = new DocumentStore { Urls = new[] { _nodes[index].WebUrl }, Conventions = { DisableTopologyUpdates = true }, Database = databaseName }.Initialize())
                    {
                        await store2.Maintenance.SendAsync(new GetStatisticsOperation());

                        Assert.True(2 == GetIdleCount(store.Database), string.Join(Environment.NewLine, wakeUpReasons.Select(x => string.Join(": ", x.Key, string.Join(", ", x.Value)))));
                        using (var s = store2.OpenAsyncSession())
                        {
                            await s.StoreAsync(new User() { Name = "Egor" }, "foo/bar");
                            await s.SaveChangesAsync();
                        }
                    }

                    count = RavenDB_13987.WaitForCount(TimeSpan.FromSeconds(300), 0, () => GetIdleCount(store.Database));
                    Assert.True(0 == count, string.Join(Environment.NewLine, wakeUpReasons.Select(x => string.Join(": ", x.Key, string.Join(", ", x.Value)))));

                    var timeout = 5000;
                    foreach (var server in _nodes)
                    {
                        using (var store2 = new DocumentStore { Urls = new[] { server.WebUrl }, Conventions = { DisableTopologyUpdates = true }, Database = databaseName }.Initialize())
                        {
                            Assert.True(WaitForDocument(store2, "foo/bar", timeout, databaseName), $"WaitForDocument for {server.ServerStore.NodeTag} returned false, after {timeout}, leader: {cluster.Leader}");
                        }
                    }

                    index = rnd.Next(0, clusterSize);
                    var nextNow = DateTime.Now + TimeSpan.FromSeconds(300);
                    var now = DateTime.Now;
                    using (var store2 = new DocumentStore { Urls = new[] { _nodes[index].WebUrl }, Conventions = { DisableTopologyUpdates = true }, Database = databaseName }.Initialize())
                    {
                        while (now < nextNow && GetIdleCount(store.Database) < 2)
                        {
                            await Task.Delay(2000);
                            await store2.Maintenance.SendAsync(new GetStatisticsOperation());
                            now = DateTime.Now;
                        }
                    }
                    Assert.True(2 == GetIdleCount(store.Database), string.Join(Environment.NewLine, wakeUpReasons.Select(x => string.Join(": ", x.Key, string.Join(", ", x.Value)))));
                }
            }
            finally
            {
                foreach (var server in _nodes)
                {
                    server.ServerStore.DatabaseIdleManager.ForTestingPurposesOnly().SkipShouldContinueDisposeCheck = false;
                    server.ServerStore.DatabasesLandlord.ForTestingPurposes = null;
                }
            }
        }

        private int GetIdleCount(string databaseName) => _nodes.Count(server => server.ServerStore.DatabaseIdleManager.GetActivityState(databaseName) is DatabaseIdleManager.DatabaseActivityState.Idle);
    }
}
