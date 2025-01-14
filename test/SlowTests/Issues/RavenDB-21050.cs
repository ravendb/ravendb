using System;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Session;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_21050 : RavenTestBase
{
    public RavenDB_21050(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.BackupExportImport)]
    [RavenData(DatabaseMode = RavenDatabaseMode.All)]
    public async Task ClusterWideTransaction_WhenRestoreFromIncrementalBackupAfterStoreAndDelete_ShouldDeleteInTheDestination(Options options)
    {
        var backupPath = NewDataPath(suffix: "BackupFolder");
        const string id = "TestObjs/0";

        using (var source = GetDocumentStore(options))
        using (var destination = new DocumentStore { Urls = new[] { Server.WebUrl }, Database = $"restored_{source.Database}" }.Initialize())
        {
            var config = new PeriodicBackupConfiguration { LocalSettings = new LocalSettings { FolderPath = backupPath }, IncrementalBackupFrequency = "0 0 */12 * *" };
            var backupTaskId = (await source.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config))).TaskId;

            using (var session = source.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
            {
                await session.StoreAsync(new TestObj(), id);
                await session.SaveChangesAsync();
            }

            await WaitAndAssertForBackup(source, options.DatabaseMode, backupTaskId);

            using (var session = source.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
            {
                session.Delete(id);
                await session.SaveChangesAsync();
            }

            var backup2Id = await WaitAndAssertForBackup(source, options.DatabaseMode, backupTaskId);

            string path;
            int? shardNumber = null;
            if (options.DatabaseMode == RavenDatabaseMode.Sharded)
            {
                shardNumber = await Sharding.GetShardNumberForAsync(source, id);
                path = Directory.GetDirectories(backupPath).First(p => p.Contains($"${shardNumber}"));
            }
            else
            {
                path = Directory.GetDirectories(backupPath).First();
            }

            await Backup.GetBackupFilesAndAssertCountAsync(backupPath, 2, backup2Id, source.Database, shardNumber);
            
            var restoreConfig = new RestoreBackupConfiguration { BackupLocation = path, DatabaseName = destination.Database };
            using (Backup.RestoreDatabase(destination, restoreConfig))
            {
                using (var session = destination.OpenAsyncSession())
                {
                    var shouldBeDeleted = await session.LoadAsync<TestObj>(id);
                    Assert.Null(shouldBeDeleted); //Fails here
                }
            }

        }
    }

    private async Task<long> WaitAndAssertForBackup(DocumentStore source, RavenDatabaseMode databaseMode, long backupTaskId, TimeSpan? timeout = null)
    {
        timeout ??= TimeSpan.FromMinutes(5);

        WaitHandle[] backupsDone = null;
        if (databaseMode == RavenDatabaseMode.Sharded)
            backupsDone = await Sharding.Backup.WaitForBackupToComplete(source);
        else
            backupsDone = await Backup.WaitForBackupToComplete(source);

        var backupStatus = await source.Maintenance.SendAsync(new StartBackupOperation(false, backupTaskId));

        Assert.True(WaitHandle.WaitAll(backupsDone, timeout.Value));

        return backupStatus.Id;
    }


    [RavenTheory(RavenTestCategory.BackupExportImport | RavenTestCategory.Sharding)]
    [InlineData(true)]
    [InlineData(false)]
    public async Task ClusterWideTransaction_Restore_FromShardedToSharded(bool delete)
    {
        var backupPath = NewDataPath(suffix: "BackupFolder");
        const string id = "TestObjs/0";

        using (var source = Sharding.GetDocumentStore())
        using (var destination = new DocumentStore { Urls = new[] { Server.WebUrl }, Database = $"restored_{source.Database}" }.Initialize())
        {
            var config = new PeriodicBackupConfiguration { LocalSettings = new LocalSettings { FolderPath = backupPath }, IncrementalBackupFrequency = "0 0 */12 * *" };
            var backupTaskId = (await source.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config))).TaskId;

            using (var session = source.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
            {
                await session.StoreAsync(new TestObj(), id);
                await session.SaveChangesAsync();
            }

            await WaitAndAssertForBackup(source, RavenDatabaseMode.Sharded, backupTaskId);

            if (delete)
            {
                using (var session = source.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    session.Delete(id);
                    await session.SaveChangesAsync();
                }

                await WaitAndAssertForBackup(source, RavenDatabaseMode.Sharded, backupTaskId);
            }

            var paths = Directory.GetDirectories(backupPath);

            using (Backup.RestoreDatabase(destination, new RestoreBackupConfiguration
                   {
                       DatabaseName = destination.Database,
                       ShardRestoreSettings = Sharding.Backup.GenerateShardRestoreSettings(paths, await Sharding.GetShardingConfigurationAsync(source))
                   }))
            using (var session = destination.OpenAsyncSession())
            {
                var shouldBeDeleted = await session.LoadAsync<TestObj>(id);
                if (delete)
                    Assert.Null(shouldBeDeleted);
                else
                    Assert.NotNull(shouldBeDeleted);
            }

        }
    }

    private class TestObj
    {
    }
}
