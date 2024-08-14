using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
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

    [RavenFact(RavenTestCategory.BackupExportImport)]
    public async Task ClusterWideTransaction_WhenRestoreFromIncrementalBackupAfterStoreAndDelete_ShouldDeleteInTheDestination()
    {
        var backupPath = NewDataPath(suffix: "BackupFolder");
        const string id = "TestObjs/0";

        using (var source = GetDocumentStore())
        using (var destination = new DocumentStore { Urls = new[] { Server.WebUrl }, Database = $"restored_{source.Database}" }.Initialize())
        {
            var config = new PeriodicBackupConfiguration { LocalSettings = new LocalSettings { FolderPath = backupPath }, IncrementalBackupFrequency = "0 0 */12 * *" };
            var backupTaskId = (await source.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config))).TaskId;

            using (var session = source.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
            {
                await session.StoreAsync(new TestObj(), id);
                await session.SaveChangesAsync();
            }

            var backupStatus = await source.Maintenance.SendAsync(new StartBackupOperation(false, backupTaskId));
            await backupStatus.WaitForCompletionAsync(TimeSpan.FromMinutes(5));
            await Backup.GetBackupFilesAndAssertCountAsync(backupPath, 1, source.Database, backupStatus.Id);
            
            using (var session = source.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
            {
                session.Delete(id);
                await session.SaveChangesAsync();
            }

            var backupStatus2 = await source.Maintenance.SendAsync(new StartBackupOperation(false, backupTaskId));
            await backupStatus2.WaitForCompletionAsync(TimeSpan.FromMinutes(5));
            await Backup.GetBackupFilesAndAssertCountAsync(backupPath, 2, source.Database, backupStatus2.Id);

            var restoreConfig = new RestoreBackupConfiguration { BackupLocation = Directory.GetDirectories(backupPath).First(), DatabaseName = destination.Database };
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

    private class TestObj
    {
    }
}
