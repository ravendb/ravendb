using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FastTests.Server.Replication;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Client.ServerWide.Operations.Configuration;
using Raven.Server.Config;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_13987 : ReplicationTestBase
    {
        public RavenDB_13987()
        {
            DoNotReuseServer();
        }

        [Fact]
        public async Task ShouldWork()
        {
            var backupPath = NewDataPath(suffix: "BackupFolder");
            const int clusterSize = 3;
            long taskId;
            var databaseName = GetDatabaseName();
            var leader = await CreateRaftClusterAndGetLeader(3, false, customSettings: new Dictionary<string, string>()
            {
                [RavenConfiguration.GetKey(x => x.Cluster.MoveToRehabGraceTime)] = "1",
                [RavenConfiguration.GetKey(x => x.Cluster.AddReplicaTimeout)] = "1",
                [RavenConfiguration.GetKey(x => x.Cluster.ElectionTimeout)] = "300",
                [RavenConfiguration.GetKey(x => x.Cluster.StabilizationTime)] = "1",
                [RavenConfiguration.GetKey(x => x.Databases.MaxIdleTime)] = "10",
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

            const int minutes = 2;
            var rnd = new Random();
            var index = rnd.Next(0, Servers.Count - 1);
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

                // wait until all dbs become idle
                var now = DateTime.Now;
                var nextNow = now + TimeSpan.FromSeconds(180);
                while (now < nextNow && GetIdleCount() < 3)
                {
                    Thread.Sleep(3000);
                    now = DateTime.Now;
                }
                Assert.Equal(3, GetIdleCount());

                var putConfiguration = new ServerWideBackupConfiguration
                {
                    FullBackupFrequency = $"*/{minutes} * * * *",
                    IncrementalBackupFrequency = $"*/{minutes} * * * *",
                    LocalSettings = new LocalSettings
                    {
                        FolderPath = backupPath
                    },
                };

                var result = await store.Maintenance.Server.SendAsync(new PutServerWideBackupConfigurationOperation(putConfiguration));
                var serverWideConfiguration = await store.Maintenance.Server.SendAsync(new GetServerWideBackupConfigurationOperation(result.Name));
                Assert.NotNull(serverWideConfiguration);

                var record1 = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                var backups1 = record1.PeriodicBackups;
                taskId = backups1.First().TaskId;

                Assert.Equal(1, backups1.Count);
            }

            Assert.Equal(3, GetIdleCount());

            // wait for the backup occurrence
            var now1 = DateTime.Now;
            var nextNow1 = now1 + TimeSpan.FromSeconds(60 * minutes + 15);
            var count = 0;
            while (now1 < nextNow1 && count < 3)
            {
                Thread.Sleep(500);
                now1 = DateTime.Now;
                count = CountOfBackupStatus(databaseName, taskId);
            }

            // should not wakeup idle dbs and should update the backup status on all dbs
            Assert.Equal(2, GetIdleCount());
            Assert.Equal(3, count);
        }

        private int CountOfBackupStatus(string databaseName, long taskId)
        {
            var count = 0;
            foreach (var server in Servers)
            {
                using (var store = new DocumentStore { Urls = new[] { server.WebUrl }, Database = databaseName }.Initialize())
                {
                    var status = store.Maintenance.Send(new GetPeriodicBackupStatusOperation(taskId)).Status;
                    if (status?.LastFullBackup != null)
                        count++;
                }
            }

            return count;
        }

        private int GetIdleCount()
        {
            return Servers.Sum(server => server.ServerStore.IdleDatabases.Count);
        }
    }
}
