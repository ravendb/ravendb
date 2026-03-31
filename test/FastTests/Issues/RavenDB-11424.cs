using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Google.Protobuf.WellKnownTypes;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Server.Utils;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Issues
{
    public class RavenDB_11424 : RavenTestBase
    {
        public RavenDB_11424(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.BackupExportImport, LicenseRequired = true)]
        public async Task CanChangeBackupFrequency()
        {
            var server = GetNewServer();
            var backupPath = NewDataPath(suffix: "BackupFolder");
            using (var store = GetDocumentStore(new Options{Server = server}))
            {
                var config = Backup.CreateBackupConfiguration(backupPath, fullBackupFrequency: "0 3 */3 * *");
                var result = await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config));
                Backup.WaitForResponsibleNodeUpdate(server.ServerStore, store.Database, result.TaskId);

                var backupRunner = server.ServerStore.BackupRunner;
                var backups = backupRunner.GetDatabaseBackups(store.Database);
                var periodicBackup = backups.First();
                var oldTimer = periodicBackup.NextBackup;
                Assert.Equal("0 3 */3 * *", periodicBackup.Configuration.FullBackupFrequency);

                config = Backup.CreateBackupConfiguration(backupPath, fullBackupFrequency: "0 2 */3 * *", taskId: result.TaskId);
                await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config));
                Assert.Equal("0 2 */3 * *", periodicBackup.Configuration.FullBackupFrequency);
                Assert.NotEqual(oldTimer, periodicBackup.NextBackup);
            }
        }

        [RavenFact(RavenTestCategory.BackupExportImport | RavenTestCategory.Sharding, LicenseRequired = true)]
        public async Task CanChangeBackupFrequency_Sharding()
        {
            var server = GetNewServer();
            var backupPath = NewDataPath(suffix: "BackupFolder");
            var options = Options.ForMode(RavenDatabaseMode.Sharded);
            options.Server = server;
            using (var store = GetDocumentStore(options))
            {
                var config = Backup.CreateBackupConfiguration(backupPath, fullBackupFrequency: "0 3 */3 * *");
                var result = await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config));

                Sharding.Backup.WaitForResponsibleNodeUpdate(server.ServerStore, store.Database, result.TaskId);
                var timers = new Dictionary<string, NextBackup>();
                List<string> shardNames = ShardHelper.GetShardNames(store.Database, Sharding.GetShardingConfiguration(store, store.Database).Shards.Keys).ToList();
                foreach (var shard in shardNames)
                {
                    var backups = server.ServerStore.BackupRunner.GetDatabaseBackups(shard);
                    var periodicBackup = backups.First();
                    var oldTimer = periodicBackup.NextBackup;
                    timers.Add(shard, oldTimer);
                    Assert.Equal("0 3 */3 * *", periodicBackup.Configuration.FullBackupFrequency);
                }

                config = Backup.CreateBackupConfiguration(backupPath, fullBackupFrequency: "0 2 */3 * *", taskId: result.TaskId);
                await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config));

                foreach (var shard in shardNames)
                {
                    var backups = server.ServerStore.BackupRunner.GetDatabaseBackups(shard);
                    var periodicBackup = backups.First();
                    var timer = periodicBackup.NextBackup;
                    Assert.NotEqual(timers[shard], timer);
                    Assert.Equal("0 2 */3 * *", periodicBackup.Configuration.FullBackupFrequency);
                }
            }
        }
    }
}
