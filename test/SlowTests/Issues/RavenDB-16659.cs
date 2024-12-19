using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Exceptions;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using SlowTests.Core.Utils.Entities;
using Sparrow.Server;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_16659 :RavenTestBase
    {

        public RavenDB_16659(ITestOutputHelper output) : base(output)
        {
        }
        [Fact]
        public async Task DeleteDatabaseDuringRestore()
        {
            DoNotReuseServer();
            var mre = new AsyncManualResetEvent();
            var mre2 = new ManualResetEvent(false);
            var backupPath = NewDataPath();
            
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Toli" },"users/1");
                    await session.SaveChangesAsync();
                }

                var operation = await store.Maintenance.SendAsync(new BackupOperation(new BackupConfiguration
                {
                    BackupType = BackupType.Backup,
                    LocalSettings = new LocalSettings
                    {
                        FolderPath = backupPath
                    }
                }));

                var result = (BackupResult)await operation.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

                var databaseName = $"{store}_Restore";
                try
                {
                    RestoreBackupOperation restoreOperation =
                        new RestoreBackupOperation(new RestoreBackupConfiguration
                            {BackupLocation = Path.Combine(backupPath, result.LocalBackup.BackupDirectory), DatabaseName = databaseName });
                    Server.ServerStore.ForTestingPurposesOnly().RestoreDatabaseAfterSavingDatabaseRecord += () => {
                        mre.Set();
                        mre2.WaitOne(); // Wait to ensure the restore process doesn't finish until we intentionally cancel it.
                    };

                    var op  = await store.Maintenance.Server.SendAsync(restoreOperation);
                    var res = await mre.WaitAsync(TimeSpan.FromSeconds(30));
                    Assert.True(res);

                    var val = await WaitForValueAsync(async () => await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(databaseName)) != null, true, 30_000);
                    Assert.True(val);
                    var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(databaseName));
                    Assert.Equal(DatabaseStateStatus.RestoreInProgress, record.DatabaseState);

                    var e = Assert.Throws<RavenException>(() => store.Maintenance.Server.Send(new DeleteDatabasesOperation(databaseName, hardDelete: true)));
                    Assert.Contains($"Can't delete database '{databaseName}' while the restore process is in progress.", e.Message);
                    mre2.Set();
                    await op.WaitForCompletionAsync(TimeSpan.FromSeconds(30));
                }
                finally
                {
                    store.Maintenance.Server.Send(new DeleteDatabasesOperation(databaseName, hardDelete: true));
                }
            }
        }
    }
}
