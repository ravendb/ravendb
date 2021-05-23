using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.Backups;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Issues
{
    public class RavenDB_11424 : RavenTestBase
    {
        public RavenDB_11424(ITestOutputHelper output) : base(output)
        {
        }

        [Fact, Trait("Category", "Smuggler")]
        public async Task CanChangeBackupFrequency()
        {
            var backupPath = NewDataPath(suffix: "BackupFolder");
            using (var store = GetDocumentStore())
            {
                var config = Backup.CreateBackupConfiguration(backupPath, fullBackupFrequency: "0 3 */3 * *");
                var result = await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config));

                var periodicBackupRunner = (await GetDocumentDatabaseInstanceFor(store)).PeriodicBackupRunner;
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
    }
}
