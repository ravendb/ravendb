using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.ServerWide.Operations;
using SlowTests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_9120 : RavenTestBase
    {
        public RavenDB_9120(ITestOutputHelper output) : base(output)
        {
        }

        [Fact, Trait("Category", "Smuggler")]
        public async Task FullAndIncrementalBackupsInSameFolderShouldWork()
        {
            var backupPath = NewDataPath(suffix: "BackupFolder");
            const string idUser = "user/1";
            const string restoredDbMane = "RestoredDB";

            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User
                    {
                        Name = "1",
                        Count = 322
                    }, idUser);
                    await session.SaveChangesAsync();
                }
                var config = Backup.CreateBackupConfiguration(backupPath);
                var backupTaskId = await Backup.UpdateConfigAndRunBackupAsync(Server, config, store);
                using (var session = store.OpenAsyncSession())
                {
                    var u = await session.LoadAsync<User>(idUser);
                    u.Name = "12";
                    await session.StoreAsync(u);
                    await session.SaveChangesAsync();
                }

                await Backup.RunBackupAsync(Server, backupTaskId, store, isFullBackup: false);
                using (var session = store.OpenAsyncSession())
                {
                    var u = await session.LoadAsync<User>(idUser);
                    u.Name = "123";
                    await session.StoreAsync(u);
                    await session.SaveChangesAsync();
                }

                await Backup.RunBackupAsync(Server, backupTaskId, store, isFullBackup: false);
                DeleteFoldersAndFiles(backupPath);

                using (var session = store.OpenAsyncSession())
                {
                    var u = await session.LoadAsync<User>(idUser);
                    u.Name = "1234";
                    await session.StoreAsync(u);
                    await session.SaveChangesAsync();
                }

                await Backup.RunBackupAsync(Server, backupTaskId, store);
                using (var session = store.OpenAsyncSession())
                {
                    var u = await session.LoadAsync<User>(idUser);
                    u.Name = "123";
                    await session.StoreAsync(u);
                    await session.SaveChangesAsync();
                }

                await Backup.RunBackupAsync(Server, backupTaskId, store, isFullBackup: false);
                using (var session = store.OpenAsyncSession())
                {
                    var u = await session.LoadAsync<User>(idUser);
                    u.Name = "12";
                    u.Count = 223;
                    await session.StoreAsync(u);
                    await session.SaveChangesAsync();
                }

                await Backup.RunBackupAsync(Server, backupTaskId, store, isFullBackup: false);
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
    }
}
