using System;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.ServerWide.Operations;
using Raven.Client.ServerWide.Operations.Configuration;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace StressTests.Server.Documents.PeriodicBackup
{
    public class PeriodicBackupTestsStress : ClusterTestBase
    {
        public PeriodicBackupTestsStress(ITestOutputHelper output) : base(output)
        {
        }

        [Fact, Trait("Category", "Smuggler")]
        public async Task FirstBackupWithClusterDownStatusShouldRearrangeTheTimer()
        {
            var backupPath = NewDataPath(suffix: "BackupFolder");
            using (var store = GetDocumentStore(new Options { DeleteDatabaseOnDispose = true, Path = NewDataPath() }))
            {
                var documentDatabase = await GetDatabase(store.Database);
                documentDatabase.PeriodicBackupRunner.ForTestingPurposesOnly().SimulateClusterDownStatus = true;

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "EGR" }, "users/1");
                    await session.SaveChangesAsync();
                }

                var config = Backup.CreateBackupConfiguration(backupPath, incrementalBackupFrequency: "* * * * *");
                var result = await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config));
                var periodicBackupTaskId = result.TaskId;
                var val = WaitForValue(() => documentDatabase.PeriodicBackupRunner._forTestingPurposes.ClusterDownStatusSimulated, true, timeout: 66666, interval: 333);
                Assert.True(val, "Failed to simulate ClusterDown Status");
                documentDatabase.PeriodicBackupRunner._forTestingPurposes = null;
                var getPeriodicBackupStatus = new GetPeriodicBackupStatusOperation(periodicBackupTaskId);
                val = WaitForValue(() => store.Maintenance.Send(getPeriodicBackupStatus).Status?.LastFullBackup != null, true, timeout: 66666, interval: 333);
                Assert.True(val, "Failed to complete the backup in time");
            }
        }

        [Fact, Trait("Category", "Smuggler")]
        public async Task ShouldRearrangeTheTimeIfBackupAfterTimerCallbackGotActiveByOtherNode()
        {
            var backupPath = NewDataPath(suffix: "BackupFolder");
            using (var server = GetNewServer())
            using (var store = GetDocumentStore(new Options
            {
                Server = server
            }))
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "EGR" }, "users/1");
                    await session.SaveChangesAsync();
                }

                while (DateTime.Now.Second > 55)
                    await Task.Delay(1000);

                await store.Maintenance.Server.SendAsync(new PutServerWideBackupConfigurationOperation(new ServerWideBackupConfiguration
                {
                    FullBackupFrequency = "*/1 * * * *",
                    LocalSettings = new LocalSettings { FolderPath = backupPath },
                }));

                var record1 = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                var backups1 = record1.PeriodicBackups;
                Assert.Equal(1, backups1.Count);

                var taskId = backups1.First().TaskId;
                var responsibleDatabase = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database).ConfigureAwait(false);
                Assert.NotNull(responsibleDatabase);
                var tag = responsibleDatabase.PeriodicBackupRunner.WhoseTaskIsIt(taskId);
                Assert.Equal(server.ServerStore.NodeTag, tag);

                responsibleDatabase.PeriodicBackupRunner.ForTestingPurposesOnly().SimulateActiveByOtherNodeStatus = true;
                var pb = responsibleDatabase.PeriodicBackupRunner.PeriodicBackups.First();
                Assert.NotNull(pb);

                var val = WaitForValue(() => pb.HasScheduledBackup(), false, timeout: 66666, interval: 444);
                Assert.False(val, "PeriodicBackup should cancel the ScheduledBackup if the task status is ActiveByOtherNode, " +
                                  "so when the task status is back to be ActiveByCurrentNode, UpdateConfigurations will be able to reassign the backup timer");

                responsibleDatabase.PeriodicBackupRunner._forTestingPurposes = null;
                responsibleDatabase.PeriodicBackupRunner.UpdateConfigurations(record1);
                var getPeriodicBackupStatus = new GetPeriodicBackupStatusOperation(taskId);

                val = WaitForValue(() => store.Maintenance.Send(getPeriodicBackupStatus).Status?.LastFullBackup != null, true, timeout: 66666, interval: 444);
                Assert.True(val, "Failed to complete the backup in time");
            }
        }
    }
}
