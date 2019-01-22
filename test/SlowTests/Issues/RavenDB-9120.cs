using System;
using System.Diagnostics;
using System.IO;
using Xunit;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.ServerWide.Operations;
using SlowTests.Core.Utils.Entities;

namespace SlowTests.Issues
{
    public class RavenDB_9120 : RavenTestBase
    {
        [Fact]
        public async Task FullAndIncrementalBackupsInSameFolderShouldWork()
        {
            var backupPath = NewDataPath(suffix: "BackupFolder");
            const string idUser = "user/1";
            const string restoredDbMane = "RestoredDB";

            using (var store = GetDocumentStore())
            {
                var result = await SetupBackupAsync(backupPath, store, BackupType.Backup, "Full Backup");
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User
                    {
                        Name = "1",
                        Count = 322
                    }, idUser);
                    await session.SaveChangesAsync();
                }

                var documentDatabase = await GetDocumentDatabaseInstanceFor(store);
                RunBackup(result.TaskId, documentDatabase, true); //full backup

                using (var session = store.OpenAsyncSession())
                {
                    var u = await session.LoadAsync<User>(idUser);
                    u.Name = "12";
                    await session.StoreAsync(u);
                    await session.SaveChangesAsync();
                }

                RunBackup(result.TaskId, documentDatabase); //incremental backup

                using (var session = store.OpenAsyncSession())
                {
                    var u = await session.LoadAsync<User>(idUser);
                    u.Name = "123";
                    await session.StoreAsync(u);
                    await session.SaveChangesAsync();
                }

                RunBackup(result.TaskId, documentDatabase); //incremental backup
                DeleteFoldersAndFiles(backupPath);

                using (var session = store.OpenAsyncSession())
                {
                    var u = await session.LoadAsync<User>(idUser);
                    u.Name = "1234";
                    await session.StoreAsync(u);
                    await session.SaveChangesAsync();
                }

                RunBackup(result.TaskId, documentDatabase, true); //full backup

                using (var session = store.OpenAsyncSession())
                {
                    var u = await session.LoadAsync<User>(idUser);
                    u.Name = "123";
                    await session.StoreAsync(u);
                    await session.SaveChangesAsync();
                }

                RunBackup(result.TaskId, documentDatabase); //incremental backup

                using (var session = store.OpenAsyncSession())
                {
                    var u = await session.LoadAsync<User>(idUser);
                    u.Name = "12";
                    u.Count = 223;
                    await session.StoreAsync(u);
                    await session.SaveChangesAsync();
                }

                RunBackup(result.TaskId, documentDatabase); //incremental backup
                var backupDirectory = Directory.GetDirectories(backupPath).First(); // get the temp folder created for backups

                store.Maintenance.Server.Send(new RestoreBackupOperation(new RestoreBackupConfiguration()
                {
                    BackupLocation = backupDirectory,
                    DatabaseName = restoredDbMane
                })).WaitForCompletion(TimeSpan.FromSeconds(30));
            }

            using (var store2 = GetDocumentStore(new Options()
            {
                CreateDatabase = false,
                ModifyDatabaseName = s => restoredDbMane
            }))
            {
                using (var session = store2.OpenAsyncSession())
                {
                    var u = await session.LoadAsync<User>(idUser);
                    Assert.NotNull(u);
                    Assert.Equal("12", u.Name);
                    Assert.Equal(223, u.Count);
                }
            }
        }

        private static void DeleteFoldersAndFiles(string backupPath)
        {
            var di = new DirectoryInfo(backupPath);

            foreach (var file in di.GetFiles())
                file.Delete();

            foreach (var dir in di.GetDirectories())
                dir.Delete(true);
        }

        private static void RunBackup(long taskId, Raven.Server.Documents.DocumentDatabase documentDatabase, bool forceFullBackup = false)
        {
            var periodicBackupRunner = documentDatabase.PeriodicBackupRunner;
            periodicBackupRunner.StartBackupTask(taskId, forceFullBackup);

            var cts = new CancellationTokenSource();
            cts.CancelAfter(Debugger.IsAttached ? 100000000 : 10000);
            while (periodicBackupRunner.HasRunningBackups() && cts.IsCancellationRequested == false)
                Thread.Sleep(100);

            if (cts.IsCancellationRequested)
                Assert.False(true, "Timed out waiting for backup. It shouldn't take more than 10 seconds to run, even on slow machine...");
        }

        private static async Task<UpdatePeriodicBackupOperationResult> SetupBackupAsync(string backupPath, DocumentStore store, BackupType backupType, string taskName)
        {
            var config = new PeriodicBackupConfiguration
            {
                LocalSettings = new LocalSettings
                {
                    FolderPath = backupPath
                },
                FullBackupFrequency = "* */6 * * *",
                IncrementalBackupFrequency = "* */6 * * *",
                BackupType = backupType
            };

            var result = await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config));
            return result;
        }
    }
}
