using System;
using System.Collections.Generic;
using System.Diagnostics;
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
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace StressTests.Issues
{
    public class RavenDB_13987 : ReplicationTestBase
    {
        private long _taskId;
        private string _databaseName;
        private readonly TimeSpan _reasonableWaitTime = Debugger.IsAttached ? TimeSpan.FromSeconds(60 * 10) : TimeSpan.FromSeconds(60 * 5);

        public RavenDB_13987(ITestOutputHelper output) : base(output)
        {
            DoNotReuseServer();
        }

        [NightlyBuildFact]
        public async Task ServerWideBackupShouldNotWakeupIdleDatabases()
        {
            var backupPath = NewDataPath(suffix: "BackupFolder");
            const int clusterSize = 3;
            _databaseName = GetDatabaseName();

            var leader = await CreateRaftClusterAndGetLeader(3, false,
                customSettings: new Dictionary<string, string>()
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
            using (var store = new DocumentStore { Urls = new[] { leader.WebUrl }, Database = _databaseName }.Initialize())
            {
                var doc = new DatabaseRecord(_databaseName);
                databaseResult = await store.Maintenance.Server.SendAsync(new CreateDatabaseOperation(doc, clusterSize));
            }

            Assert.Equal(clusterSize, databaseResult.Topology.AllNodes.Count());

            foreach (var server in Servers)
            {
                await server.ServerStore.Cluster.WaitForIndexNotification(databaseResult.RaftCommandIndex);
            }

            foreach (var server in Servers.Where(s => databaseResult.NodesAddedTo.Any(n => n == s.WebUrl)))
            {
                await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(_databaseName);
            }

            var rnd = new Random();
            var index = rnd.Next(0, Servers.Count - 1);
            using (var store = new DocumentStore { Urls = new[] { Servers[index].WebUrl }, Database = _databaseName }.Initialize())
            {
                using (var s = store.OpenAsyncSession())
                {
                    await s.StoreAsync(new User() { Name = "Egor" }, "foo/bar");
                    await s.SaveChangesAsync();
                }

                // wait until all dbs become idle
                var now = DateTime.Now;
                var nextNow = now + TimeSpan.FromSeconds(300);
                while (now < nextNow && GetIdleCount() < 3)
                {
                    await Task.Delay(3000);
                    now = DateTime.Now;
                }

                Assert.Equal(3, GetIdleCount());

                var putConfiguration = new ServerWideBackupConfiguration
                {
                    FullBackupFrequency = "*/1 * * * *",
                    IncrementalBackupFrequency = "*/1 * * * *",
                    LocalSettings = new LocalSettings { FolderPath = backupPath },
                };

                var result = await store.Maintenance.Server.SendAsync(new PutServerWideBackupConfigurationOperation(putConfiguration));
                var serverWideConfiguration = await store.Maintenance.Server.SendAsync(new GetServerWideBackupConfigurationOperation(result.Name));
                Assert.NotNull(serverWideConfiguration);

                var record1 = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                var backups1 = record1.PeriodicBackups;
                _taskId = backups1.First().TaskId;

                Assert.Equal(1, backups1.Count);
            }

            Assert.Equal(3, GetIdleCount());

            // wait for the backup occurrence
            var idleCount = WaitForCount(_reasonableWaitTime, 2, GetIdleCount);
            Assert.Equal(2, idleCount);

            // backup status should not wakeup dbs
            var count = WaitForCount(_reasonableWaitTime, 3, CountOfBackupStatus);
            Assert.Equal(3, count);
        }

        private static int WaitForCount(TimeSpan seconds, int excepted, Func<int> func)
        {
            var now = DateTime.Now;
            var nextNow = now + seconds;
            var count = func();
            while (now < nextNow && count != excepted)
            {
                Thread.Sleep(500);
                now = DateTime.Now;
                count = func();
            }

            return count;
        }

        private int CountOfBackupStatus()
        {
            var count = 0;
            foreach (var server in Servers)
            {
                using (var store = new DocumentStore { Urls = new[] { server.WebUrl }, Conventions = { DisableTopologyUpdates = true }, Database = _databaseName }.Initialize())
                {
                    var status = store.Maintenance.Send(new GetPeriodicBackupStatusOperation(_taskId)).Status;
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
