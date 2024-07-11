using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Orders;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.ServerWide.Operations;
using Raven.Server;
using Raven.Server.Config;
using Raven.Server.Extensions;
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
            Backup.WaitForResponsibleNodeUpdateInCluster(store, nodes, result.TaskId);

            // Turn Database offline in second server.
            var unloaded = secondServer.ServerStore.DatabasesLandlord.UnloadDirectly(store.Database);
            Assert.True(unloaded);

            // Backup run in first server
            await Backup.RunBackupAsync(firstServer, result.TaskId, store);

            // Test - check if the second server backup info is up to date
            BackupInfo firstInfo = default;
            BackupInfo secondInfo = default;

            await WaitForValueAsync(async () =>
            {
                firstInfo = await GetBackupInfo(firstServer);
                if (firstInfo == null)
                    return false;

                secondInfo = await GetBackupInfo(secondServer);
                if (secondInfo == null)
                    return false;

                return (Math.Abs(firstInfo.IntervalUntilNextBackupInSec - secondInfo.IntervalUntilNextBackupInSec) <= 1) && 
                       firstInfo.LastBackup == secondInfo.LastBackup;
            }, true, timeout: 60_000, interval: 1000);

            Assert.Equal(firstInfo.BackupTaskType, secondInfo.BackupTaskType);
            Assert.Equal(firstInfo.IntervalUntilNextBackupInSec, secondInfo.IntervalUntilNextBackupInSec, tolerance: 1);
            Assert.Equal(firstInfo.Destinations?.Count, 1);
            Assert.Equal(secondInfo.Destinations?.Count, 1);
            Assert.Equal(firstInfo.Destinations?[0], secondInfo.Destinations?[0]);
        }

        private async Task<BackupInfo> GetBackupInfo(RavenServer server)
        {
            using var client = new HttpClient().WithConventions(DocumentConventions.DefaultForServer);
            var res = await client.GetAsync($"{server.WebUrl}/databases");
            string resBodyJson = await res.Content.ReadAsStringAsync();
            var resBody = JsonConvert.DeserializeObject<ResBody>(resBodyJson);
            Assert.Equal(resBody?.Databases?.Length, 1);
            return resBody?.Databases?[0].BackupInfo;
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
