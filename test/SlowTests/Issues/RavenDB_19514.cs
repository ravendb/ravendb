using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Operations.Backups;
using SlowTests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_19514 : RavenTestBase
{
    public RavenDB_19514(ITestOutputHelper output) : base(output)
    {
    }

    [Fact, Trait("Category", "Smuggler")]
    public async Task FullBackupShouldNotBackupTombstones()
    {
        const string userId = "user/1";
        var backupPath = NewDataPath(suffix: "BackupFolder");
        var config = Backup.CreateBackupConfiguration(backupPath);

        using (var store = GetDocumentStore())
        using (var session = store.OpenAsyncSession())
        {
            await session.StoreAsync(new User { Name = "Lev" }, userId);
            await session.SaveChangesAsync();

            var backupTaskId = await Backup.UpdateConfigAndRunBackupAsync(Server, config, store);

            var loadedUser = await session.LoadAsync<User>(userId);
            session.Delete(loadedUser);
            await session.SaveChangesAsync();

            // Incremental backups are including tombstones
            var backupOperation = store.Maintenance.ForDatabase(store.Database).Send(new StartBackupOperation(isFullBackup: false, taskId: backupTaskId));
            var backupResult = await backupOperation.WaitForCompletionAsync() as BackupResult;
            Assert.NotNull(backupResult);
            Assert.Equal(1, backupResult.Tombstones.ReadCount);

            // But full backups does not
            backupOperation = store.Maintenance.ForDatabase(store.Database).Send(new StartBackupOperation(isFullBackup: true, taskId: backupTaskId));
            backupResult = await backupOperation.WaitForCompletionAsync() as BackupResult;
            Assert.NotNull(backupResult);
            Assert.Equal(0, backupResult.Tombstones.ReadCount);
        }
    }
}
