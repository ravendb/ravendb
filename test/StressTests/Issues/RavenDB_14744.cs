using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests.Server.Replication;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations;
using Raven.Server;
using Raven.Server.Config;
using Xunit;
using Xunit.Abstractions;

namespace StressTests.Issues
{
    public class RavenDB_14744 : ReplicationTestBase
    {
        public RavenDB_14744(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task LoadingIdleDatabaseShouldNotMoveToRehab()
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

            var nodes = cluster.Nodes;

            try
            {
                foreach (var server in nodes)
                {
                    server.ServerStore.DatabasesLandlord.SkipShouldContinueDisposeCheck = true;
                }

                using (var store = GetDocumentStore(new Options
                {
                    ModifyDatabaseName = s => databaseName,
                    ReplicationFactor = clusterSize,
                    Server = cluster.Leader,
                    RunInMemory = false
                }))
                {
                    var rnd = new Random();
                    var index = rnd.Next(0, clusterSize);

                    foreach (var server in nodes)
                    {
                        server.ServerStore.DatabasesLandlord.ForTestingPurposesOnly().HoldDocumentDatabaseCreation = 3000;
                    }

                    var count = RavenDB_13987.WaitForCount(TimeSpan.FromSeconds(300), clusterSize, () => GetIdleCount(nodes));
                    Assert.Equal(clusterSize, count);

                    foreach (var server in nodes)
                    {
                        Assert.Equal(1, server.ServerStore.IdleDatabases.Count);
                        Assert.True(server.ServerStore.IdleDatabases.TryGetValue(databaseName, out var dictionary));

                        // new incoming replications not saved in IdleDatabases
                        Assert.Equal(0, dictionary.Count);
                    }

                    using (var store2 = new DocumentStore { Urls = new[] { nodes[index].WebUrl }, Conventions = { DisableTopologyUpdates = true }, Database = databaseName }.Initialize())
                    {
                        await store2.Maintenance.SendAsync(new GetStatisticsOperation());

                        Assert.True(await WaitForValueAsync(() => nodes.Any(x => x.ServerStore.DatabasesLandlord.ForTestingPurposesOnly().PreventedRehabOfIdleDatabase), true),
                            "await WaitForValueAsync(() => _nodes.Any(x => x.ServerStore.DatabasesLandlord.ForTestingPurposesOnly().PreventedRehabOfIdleDatabase), true)");

                        Assert.Equal(2, GetIdleCount(nodes));
                    }
                }
            }
            finally
            {
                foreach (var server in nodes)
                {
                    server.ServerStore.DatabasesLandlord.SkipShouldContinueDisposeCheck = false;
                }
            }
        }

        private static int GetIdleCount(IEnumerable<RavenServer> nodes)
        {
            return nodes.Sum(server => server.ServerStore.IdleDatabases.Count);
        }
    }
}
