using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using FastTests;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Orders;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Backups;
using Raven.Server.Config;
using Raven.Server.Config.Settings;
using Raven.Server.Documents;
using Raven.Server.Monitoring.Snmp;
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
            var clusterSize = 2;
            var (nodes, leader) = await CreateRaftCluster(clusterSize, customSettings: new Dictionary<string, string>
            {
                [RavenConfiguration.GetKey(x => x.Databases.MaxIdleTime)] = "10",
                [RavenConfiguration.GetKey(x => x.Databases.FrequencyToCheckForIdle)] = "3",
                [RavenConfiguration.GetKey(x => x.Core.RunInMemory)] = "false",
            });
            foreach (var node in nodes)
            {
                node.ServerStore.DatabasesLandlord.SkipShouldContinueDisposeCheck = true;
            }

            var storeOptions = new Options
            {
                Server = leader,
                ReplicationFactor = clusterSize,
                RunInMemory = false,
                ModifyDocumentStore = (store) => store.Conventions.DisableTopologyUpdates = true
            };
            using var store = GetDocumentStore(storeOptions);
            string firstNodeUrl = null;
            Category c = new Category { Name = $"n0", Description = $"d0" };
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(c);
                session.Advanced.WaitForReplicationAfterSaveChanges(replicas: clusterSize-1);
                session.Advanced.RequestExecutor.OnSucceedRequest += (sender, args) =>
                {
                    var uri = new Uri(args.Url);
                    firstNodeUrl = $"http://{uri.Host}:{uri.Port}";
                };
                await session.SaveChangesAsync();
            }
            var firstServer = nodes.Single(n => n.ServerStore.GetNodeHttpServerUrl() == firstNodeUrl);
            var secondServer = nodes.Single(n => n != firstServer);

            // Backup Config
            var config = Backup.CreateBackupConfiguration(backupPath, mentorNode: firstServer.ServerStore.NodeTag);
            var result = await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config));

            // Turn Database offline in second server.
            Console.WriteLine($"{secondServer.ServerStore.GetNodeHttpServerUrl()}/studio/index.html#databases");
            Assert.Equal(1, WaitForValue(() => secondServer.ServerStore.IdleDatabases.Count, 1, timeout: 60000, interval: 1000)); //wait for db to be idle
            var online = secondServer.ServerStore.DatabasesLandlord.DatabasesCache.TryGetValue(store.Database, out Task<DocumentDatabase> dbTask) &&
                         dbTask != null &&
                         dbTask.IsCompleted;
            Console.WriteLine($"Online: {online} {secondServer.ServerStore.IdleDatabases.Count}");

            // Backup run
            await Backup.RunBackupAsync(firstServer, result.TaskId, store);

            // Test
            using var client = new HttpClient();
            var res = await client.GetAsync($"{secondServer.WebUrl}/databases");
            string resBodyJson = await res.Content.ReadAsStringAsync();
            var resBody = JsonConvert.DeserializeObject<ResBody>(resBodyJson);
            Assert.NotNull(resBody);
            Assert.NotNull(resBody.Databases);
            Assert.Equal(resBody.Databases.Length, 1);
            Assert.NotNull(resBody.Databases[0].BackupInfo);
        }

        private class TestObj
        {
            public string Id { get; set; }
            public string Content { get; set; }
        }

        private class ResBody
        {
            public Database[] Databases { get; set; }

            public class Database
            {
                public JObject BackupInfo { get; set; }
            }
        }
    }
}
