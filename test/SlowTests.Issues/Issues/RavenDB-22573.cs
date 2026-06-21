using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Operations.Backups;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_22573 :RavenTestBase
    {
        public RavenDB_22573(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.BackupExportImport)]
        public async Task GetNextBackupTime()
        {
            var backupPath = NewDataPath(suffix: "BackupFolder");
            using (var store = GetDocumentStore())
            {
                var config = Backup.CreateBackupConfiguration(backupPath, fullBackupFrequency: "0 1 * * *", backupType: BackupType.Backup, disabled: false);
                var id = await Backup.UpdateConfigAndRunBackupAsync(Server, config, store);
                var documentDatabase = await Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
                var status = documentDatabase.ServerStore.BackupRunner.GetMostUpdatedClusterBackupStatus(documentDatabase.Name, id);
                config.TaskId = id;
                var nextBackupDetails = documentDatabase.ServerStore.BackupRunner.GetNextBackupDetails(id, documentDatabase.Name, out string _);
                var nextBackup = nextBackupDetails.DateTime.ToLocalTime();

                Assert.Equal(1,nextBackup.Hour);
            }
        }
    }
}
