using System;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Operations.Backups;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_11439 : RavenTestBase
    {
        [Fact]
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

                var config = new PeriodicBackupConfiguration
                {
                    LocalSettings = new LocalSettings
                    {
                        FolderPath = backupPath
                    },
                    IncrementalBackupFrequency = "0 3 */3 * *"
                };

                var backupTaskId = (await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config))).TaskId;
                await store.Maintenance.SendAsync(new StartBackupOperation(true, backupTaskId));

                var getPeriodicBackupStatus = new GetPeriodicBackupStatusOperation(backupTaskId);
                SpinWait.SpinUntil(() =>
                {
                    var getPeriodicBackupResult = store.Maintenance.Send(getPeriodicBackupStatus);
                    return getPeriodicBackupResult.Status?.LastEtag > 0;
                }, TimeSpan.FromSeconds(15));

                var oldFolderName = store.Maintenance.Send(getPeriodicBackupStatus).Status.FolderName;
                Assert.NotNull(oldFolderName);

                await store.Maintenance.SendAsync(new StartBackupOperation(false, backupTaskId));
                getPeriodicBackupStatus = new GetPeriodicBackupStatusOperation(backupTaskId);
                SpinWait.SpinUntil(() =>
                {
                    var getPeriodicBackupResult = store.Maintenance.Send(getPeriodicBackupStatus);
                    return getPeriodicBackupResult.Status?.LastIncrementalBackup != null;
                }, TimeSpan.FromSeconds(15));

                var newfolderName = store.Maintenance.Send(getPeriodicBackupStatus).Status.FolderName;
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
