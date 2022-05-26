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
            var customSettingsList = new List<IDictionary<string, string>>() {
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

            var (nodes, leader) = await CreateRaftCluster(numberOfNodes: 2, leaderIndex: 0, customSettingsList: customSettingsList);

            foreach (var node in nodes)
            {
                node.ServerStore.DatabasesLandlord.SkipShouldContinueDisposeCheck = true;
            }

            return nodes;
        }

    }
}
