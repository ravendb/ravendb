using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.Backups;
using Xunit;

namespace FastTests.Issues
{
    public class RavenDB_11424 : RavenTestBase
    {
        [Fact]
        public async Task CanChangeBackupFrequency()
        {
            var backupPath = NewDataPath(suffix: "BackupFolder");
            using (var store = GetDocumentStore())
            {
                var config = new PeriodicBackupConfiguration
                {
                    LocalSettings = new LocalSettings
                    {
                        FolderPath = backupPath
                    },
                    FullBackupFrequency = "0 3 */3 * *",
                };

                var result = await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config));

                var periodicBackupRunner = (await GetDocumentDatabaseInstanceFor(store)).PeriodicBackupRunner;
                var backups = periodicBackupRunner.PeriodicBackups;
                var periodicBackup = backups.First();
                var oldTimer = periodicBackup.BackupTimer;
                Assert.Equal("0 3 */3 * *", periodicBackup.Configuration.FullBackupFrequency);

                config = new PeriodicBackupConfiguration
                {
                    TaskId = result.TaskId,
                    LocalSettings = new LocalSettings
                    {
                        FolderPath = backupPath
                    },
                    FullBackupFrequency = "0 2 */3 * *",
                };
                await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config));
                Assert.NotEqual(oldTimer, periodicBackup.BackupTimer);
                Assert.Equal("0 2 */3 * *", periodicBackup.Configuration.FullBackupFrequency);
            }
        }
    }
}
