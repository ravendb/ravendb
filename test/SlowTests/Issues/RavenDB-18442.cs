using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Orders;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Server;
using Raven.Server.Config;
using Raven.Server.Documents;
using Raven.Server.Rachis;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_18442 : ClusterTestBase
    {
        public RavenDB_18442(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task BackupInfoShouldBeInSyncBetweenClusterNodes()
        {
            var backupPath = NewDataPath(suffix: "BackupFolder");

            // Setup Db
            var nodes = await CreateMyCluster();
            var firstServer = nodes[0]; // leader
            var secondServer = nodes[1]; // no leader

            using var store = GetDocumentStore(new Options
            {
                Server = nodes[0],
                ReplicationFactor = 2,
                RunInMemory = false,
                ModifyDocumentStore = (store) => store.Conventions.DisableTopologyUpdates = true
            });

            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new Category { Name = $"n0", Description = $"d0" });
                session.Advanced.WaitForReplicationAfterSaveChanges(replicas: 1);
                await session.SaveChangesAsync();
            }

            // Backup Config
            var config = Backup.CreateBackupConfiguration(backupPath, mentorNode: firstServer.ServerStore.NodeTag);
            var result = await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config));

            // Turn Database offline in second server.
            Assert.Equal(1, WaitForValue(() => secondServer.ServerStore.IdleDatabases.Count, 1, timeout: 60000, interval: 1000)); //wait for db to be idle
            var online = secondServer.ServerStore.DatabasesLandlord.DatabasesCache.TryGetValue(store.Database, out Task<DocumentDatabase> dbTask) &&
                         dbTask != null &&
                         dbTask.IsCompleted;
            Assert.False(online);

            // Backup run in first server
            await Backup.RunBackupAsync(firstServer, result.TaskId, store);

            // Test - check if the second server backup info is up to date
            var firstInfo = await GetBackupInfo(firstServer);
            var secondInfo = await GetBackupInfo(secondServer);

            Assert.NotNull(firstInfo);
            Assert.NotNull(secondInfo);
            Assert.Equal(firstInfo.LastBackup, secondInfo.LastBackup);
            Assert.Equal(firstInfo.BackupTaskType, secondInfo.BackupTaskType);
            Assert.Equal(((int)(firstInfo.IntervalUntilNextBackupInSec)) / 60, ((int)(secondInfo.IntervalUntilNextBackupInSec)) / 60);
            Assert.NotNull(firstInfo.Destinations);
            Assert.NotNull(secondInfo.Destinations);
            Assert.Equal(firstInfo.Destinations.Count, 1);
            Assert.Equal(secondInfo.Destinations.Count, 1);
            Assert.Equal(firstInfo.Destinations[0], secondInfo.Destinations[0]);
        }

        private async Task<BackupInfo> GetBackupInfo(RavenServer server)
        {
            using var client = new HttpClient();
            var res = await client.GetAsync($"{server.WebUrl}/databases");
            string resBodyJson = await res.Content.ReadAsStringAsync();
            var resBody = JsonConvert.DeserializeObject<ResBody>(resBodyJson);
            Assert.NotNull(resBody);
            Assert.NotNull(resBody.Databases);
            Assert.Equal(resBody.Databases.Length, 1);
            return resBody.Databases[0].BackupInfo;
        }


        private class ResBody
        {
            public Database[] Databases { get; set; }

            public class Database
            {
                public BackupInfo BackupInfo { get; set; }
            }
        }

        protected async Task<List<RavenServer>> CreateMyCluster()
        {
            int numberOfNodes = 2;
            var customSettingsArr = new IDictionary<string, string>[] {
                new Dictionary<string, string>(DefaultClusterSettings)
                {
                    [RavenConfiguration.GetKey(x => x.Cluster.ElectionTimeout)] = "300",
                },
                new Dictionary<string, string>
                {
                    [RavenConfiguration.GetKey(x => x.Databases.MaxIdleTime)] = "10",
                    [RavenConfiguration.GetKey(x => x.Databases.FrequencyToCheckForIdle)] = "3",
                    [RavenConfiguration.GetKey(x => x.Core.RunInMemory)] = "false",
                }
            };


            string[] allowedNodeTags = { "A", "B" };
            int leaderIndex = 0;
            RavenServer leader = null;
            var serversToPorts = new Dictionary<RavenServer, string>();
            var clusterNodes = new List<RavenServer>(); // we need this in case we create more than 1 cluster in the same test

            for (var i = 0; i < numberOfNodes; i++)
            {
                var customSettings = customSettingsArr[i];

                var serverUrl = UseFiddlerUrl("http://127.0.0.1:0");
                customSettings[RavenConfiguration.GetKey(x => x.Core.ServerUrls)] = serverUrl;

                var co = new ServerCreationOptions
                {
                    CustomSettings = customSettings,
                    RunInMemory = null,
                    RegisterForDisposal = false,
                    NodeTag = allowedNodeTags[i]
                };
                var server = GetNewServer(co, null);
                var port = Convert.ToInt32(server.ServerStore.GetNodeHttpServerUrl().Split(':')[2]);
                serverUrl = UseFiddlerUrl($"http://127.0.0.1:{port}");
                Servers.Add(server);
                clusterNodes.Add(server);

                serversToPorts.Add(server, serverUrl);
                if (i == leaderIndex)
                {
                    await server.ServerStore.EnsureNotPassiveAsync(null, nodeTag: co.NodeTag);
                    leader = server;
                }

                server.ServerStore.DatabasesLandlord.SkipShouldContinueDisposeCheck = true;
            }

            using (var cts = new CancellationTokenSource(TimeSpan.FromMinutes(5)))
            {
                for (var i = 0; i < numberOfNodes; i++)
                {
                    if (i == leaderIndex)
                    {
                        continue;
                    }
                    var follower = clusterNodes[i];
                    // ReSharper disable once PossibleNullReferenceException
                    leader = await ActionWithLeader(l =>
                        l.ServerStore.AddNodeToClusterAsync(serversToPorts[follower], nodeTag: allowedNodeTags[i], asWatcher: false, token: cts.Token), clusterNodes);

                    await follower.ServerStore.WaitForTopology(Leader.TopologyModification.Voter, cts.Token);

                }
            }

            // wait for cluster topology on all nodes
            foreach (var node in clusterNodes)
            {
                var nodesInTopology = await WaitForValueAsync(async () => await Task.FromResult(node.ServerStore.GetClusterTopology().AllNodes.Count), clusterNodes.Count, interval: 444);
                Assert.Equal(clusterNodes.Count, nodesInTopology);
            }

            // ReSharper disable once PossibleNullReferenceException
            var condition = await leader.ServerStore.WaitForState(RachisState.Leader, CancellationToken.None).WaitWithoutExceptionAsync(3000);
            var states = "The leader has changed while waiting for cluster to become stable. All nodes status: ";
            if (condition == false)
            {
                InvalidOperationException e = null;

                // leader changed, try get the new leader if no leader index was selected
                try
                {
                    await ActionWithLeader(_ => Task.CompletedTask, clusterNodes);
                    return clusterNodes;
                }
                catch (InvalidOperationException ex)
                {
                    e = ex;
                }

                states += Cluster.GetLastStatesFromAllServersOrderedByTime();
                if (e != null)
                    states += $"{Environment.NewLine}{e}";
            }
            Assert.True(condition, states);
            return clusterNodes;
        }

    }
}
