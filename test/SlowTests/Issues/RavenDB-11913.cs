using System.IO;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Smuggler;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_11913 : RavenTestBase
    {
        [Fact, Trait("Category", "Smuggler")]
        public async Task can_restore_legacy_backup()
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
                    IncrementalBackupFrequency = "* * * * *" //every minute
                };

                var backupTaskId = (await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config))).TaskId;
                await store.Maintenance.SendAsync(new StartBackupOperation(true, backupTaskId));
                var operation = new GetPeriodicBackupStatusOperation(backupTaskId);
                var value = WaitForValue(() =>
                {
                    var status = store.Maintenance.Send(operation).Status;
                    return status?.LastEtag;
                }, 1);
                Assert.Equal(1, value);

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "ayende" }, "users/2");
                    await session.SaveChangesAsync();
                }

                var lastEtag = store.Maintenance.Send(new GetStatisticsOperation()).LastDocEtag;
                await store.Maintenance.SendAsync(new StartBackupOperation(false, backupTaskId));
                value = WaitForValue(() => store.Maintenance.Send(operation).Status.LastEtag, lastEtag);
                Assert.Equal(lastEtag, value);
            }

            using (var store = GetDocumentStore(new Options
            {
                ModifyDatabaseName = s => $"{s}_2"
            }))
            {
                var directory = Directory.GetDirectories(backupPath).First();
                var files = Directory.GetFiles(directory);
                Assert.Equal(2, files.Length);

                File.Move(files[0], files[0].Replace(Constants.Documents.PeriodicBackup.FullBackupExtension, ".ravendb-full-dump"));
                File.Move(files[1], files[1].Replace(Constants.Documents.PeriodicBackup.IncrementalBackupExtension, ".ravendb-incremental-dump"));

                await store.Smuggler.ImportIncrementalAsync(new DatabaseSmugglerImportOptions(),
                    directory);
                using (var session = store.OpenAsyncSession())
                {
                    var users = await session.LoadAsync<User>(new[] { "users/1", "users/2" });
                    Assert.True(users.Any(x => x.Value.Name == "oren"));
                    Assert.True(users.Any(x => x.Value.Name == "ayende"));
                }
            }
        }

    }
}
