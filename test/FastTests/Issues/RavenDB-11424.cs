using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.Backups;
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

        [RavenFact(RavenTestCategory.BackupExportImport)]
        public async Task CanChangeBackupFrequency()
        {
            var backupPath = NewDataPath(suffix: "BackupFolder");
            using (var store = GetDocumentStore())
            {
                var config = Backup.CreateBackupConfiguration(backupPath, fullBackupFrequency: "0 3 */3 * *");
                var result = await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config));

                var periodicBackupRunner = (await Databases.GetDocumentDatabaseInstanceFor(store)).PeriodicBackupRunner;
                var backups = periodicBackupRunner.PeriodicBackups;
                var periodicBackup = backups.First();
                var oldTimer = periodicBackup.GetTimer();
                Assert.Equal("0 3 */3 * *", periodicBackup.Configuration.FullBackupFrequency);

                config = Backup.CreateBackupConfiguration(backupPath, fullBackupFrequency: "0 2 */3 * *", taskId: result.TaskId);
                await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config));
                Assert.NotEqual(oldTimer, periodicBackup.GetTimer());
                Assert.Equal("0 2 */3 * *", periodicBackup.Configuration.FullBackupFrequency);
            }
        }

        [RavenFact(RavenTestCategory.BackupExportImport | RavenTestCategory.Sharding)]
        public async Task CanChangeBackupFrequency_Sharding()
        {
            var backupPath = NewDataPath(suffix: "BackupFolder");
            using (var store = GetDocumentStore(Options.ForMode(RavenDatabaseMode.Sharded)))
            {
                var config = Backup.CreateBackupConfiguration(backupPath, fullBackupFrequency: "0 3 */3 * *");
                var result = await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config));

                var timers = new Dictionary<int, Timer>();
                await foreach (var shard in Sharding.GetShardsDocumentDatabaseInstancesFor(store.Database))
                {
                    var backups = shard.PeriodicBackupRunner.PeriodicBackups;
                    var periodicBackup = backups.First();
                    timers.Add(shard.ShardNumber, periodicBackup.GetTimer());
                    Assert.Equal("0 3 */3 * *", periodicBackup.Configuration.FullBackupFrequency);
                }
                
                config = Backup.CreateBackupConfiguration(backupPath, fullBackupFrequency: "0 2 */3 * *", taskId: result.TaskId);
                await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config));

                await foreach (var shard in Sharding.GetShardsDocumentDatabaseInstancesFor(store.Database))
                {
                    var backups = shard.PeriodicBackupRunner.PeriodicBackups;
                    var periodicBackup = backups.First();
                    Assert.NotEqual(timers[shard.ShardNumber], periodicBackup.GetTimer());
                    Assert.Equal("0 2 */3 * *", periodicBackup.Configuration.FullBackupFrequency);
                }
            }
        }
    }
}
