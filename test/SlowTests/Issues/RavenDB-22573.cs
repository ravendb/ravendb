using System;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Operations.Backups;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;
using static Raven.Server.Utils.BackupUtils;

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
                var periodicBackupRunner = (await Databases.GetDocumentDatabaseInstanceFor(store)).PeriodicBackupRunner;
                var config = Backup.CreateBackupConfiguration(backupPath, fullBackupFrequency: "0 1 * * *", backupType: BackupType.Backup, disabled: false);
                var id = await Backup.UpdateConfigAndRunBackupAsync(Server, config, store);
                var documentDatabase = await Databases.GetDocumentDatabaseInstanceFor(store);
                var status = documentDatabase.PeriodicBackupRunner.GetBackupStatus(id);
                config.TaskId = id;
                var nextBackupDetails = periodicBackupRunner.GetNextBackupDetails(config, status, out string _);
                var nextBackup = nextBackupDetails.DateTime.ToLocalTime();

                Assert.Equal(1,nextBackup.Hour);
            }
        }
    }
}
