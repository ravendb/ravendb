using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.PeriodicBackup
{
    public class RavenDB_20544 : RavenTestBase
    {
        public RavenDB_20544(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task CanRunTwoBackupsConcurrently()
        {
            var backupPath = NewDataPath(suffix: "BackupFolder");
            DoNotReuseServer();
            await Server.ServerStore.EnsureNotPassiveAsync();

            using var store = GetDocumentStore();
            var documentDatabase = await Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
            Assert.NotNull(documentDatabase);

            var tcs = new TaskCompletionSource<object>(TaskCreationOptions.RunContinuationsAsynchronously);
            await Backup.HoldBackupExecutionIfNeededAndInvoke(documentDatabase.PeriodicBackupRunner.ForTestingPurposesOnly(), async () =>
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Lev" });
                    await session.SaveChangesAsync();
                }

                var config1 = Backup.CreateBackupConfiguration(backupPath, fullBackupFrequency: "* * * * *");
                var config2 = Backup.CreateBackupConfiguration(backupPath, backupType: BackupType.Snapshot);

                var taskId1 = await Backup.UpdateConfigAndRunBackupAsync(Server, config1, store, opStatus: OperationStatus.InProgress);
                var taskId2 = await Backup.UpdateConfigAndRunBackupAsync(Server, config2, store, opStatus: OperationStatus.InProgress);

                var op1 = new GetOngoingTaskInfoOperation(taskId1, OngoingTaskType.Backup);
                var op2 = new GetOngoingTaskInfoOperation(taskId2, OngoingTaskType.Backup);

                var backupResult1 = store.Maintenance.Send(op1) as OngoingTaskBackup;
                var backupResult2 = store.Maintenance.Send(op2) as OngoingTaskBackup;

                Assert.NotNull(backupResult1?.OnGoingBackup);
                Assert.NotNull(backupResult2?.OnGoingBackup);

                tcs.TrySetResult(null);
            }, tcs);
        }
    }
}
