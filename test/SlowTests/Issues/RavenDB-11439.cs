using System;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Operations.Backups;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_11439 : RavenTestBase
    {
        public RavenDB_11439(ITestOutputHelper output) : base(output)
        {
        }

        [Fact, Trait("Category", "Smuggler")]
        public async Task can_continue_incremental_backup_with_same_folder()
        {
            var backupPath = NewDataPath(suffix: "BackupFolder");
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "oren" }, "users/1");
                    await session.SaveChangesAsync();
                }

                var config = Backup.CreateBackupConfiguration(backupPath, incrementalBackupFrequency: "0 3 */3 * *");
                var backupTaskId = Backup.UpdateConfigAndRunBackup(Server, config, store);
                var getPeriodicBackupStatus = new GetPeriodicBackupStatusOperation(backupTaskId);
                var oldFolderName = store.Maintenance.Send(getPeriodicBackupStatus).Status.FolderName;
                Assert.NotNull(oldFolderName);

                var status = await Backup.RunBackupAndReturnStatusAsync(Server, backupTaskId, store, isFullBackup: false);
                var newfolderName = status.FolderName;
                Assert.NotNull(newfolderName);
                Assert.Equal(oldFolderName, newfolderName);
            }
        }

        private class User
        {
            public string Name { get; set; }
        }
    }
}
