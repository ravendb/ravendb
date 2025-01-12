using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.ServerWide.Operations;
using Raven.Client.ServerWide.Operations.Configuration;
using Raven.Server;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;
using static Raven.Server.Utils.BackupUtils;

namespace StressTests.Server.Documents.PeriodicBackup
{
    public class PeriodicBackupTestsStress : ClusterTestBase
    {
        public PeriodicBackupTestsStress(ITestOutputHelper output) : base(output)
        {
        }

        [Fact, Trait("Category", "Smuggler")]
        public async Task WillRunBackupAfterGettingMissingResponsibleNode()
        {
            var backupPath = NewDataPath(suffix: "BackupFolder");

            using (var store = GetDocumentStore(new Options { DeleteDatabaseOnDispose = true, Path = NewDataPath() }))
            {
                var gotMissingResponsibleNode = false;
                var documentDatabase = await GetDatabase(store.Database);
                documentDatabase.PeriodicBackupRunner.ForTestingPurposesOnly().OnMissingResponsibleNode = () => gotMissingResponsibleNode = true;

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "EGR" }, "users/1");
                    await session.SaveChangesAsync();
                }

                var config = Backup.CreateBackupConfiguration(backupPath, incrementalBackupFrequency: "* * * * *");
                var result = await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config));
                var periodicBackupTaskId = result.TaskId;

                var val = WaitForValue(() => gotMissingResponsibleNode, true, timeout: 66666, interval: 333);
                Assert.True(val, "Didn't get a missing responsible node status");

                Backup.WaitForResponsibleNodeUpdate(Server.ServerStore, store.Database, periodicBackupTaskId);

                var getPeriodicBackupStatus = new GetPeriodicBackupStatusOperation(periodicBackupTaskId);
                PeriodicBackupStatus getStatusResult = null;
                val = WaitForValue(() =>
                {
                    getStatusResult = store.Maintenance.Send(getPeriodicBackupStatus).Status;
                    return getStatusResult?.LastFullBackup != null;
                }, true, timeout: 66666, interval: 333);

                Assert.True(val, BuildFailedToCompleteBackupMessage());
                return;

                string BuildFailedToCompleteBackupMessage()
                {
                    var msg = "Failed to complete the backup in time.";

                    using (Server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                    using (context.OpenReadTransaction())
                    {
                        var responsibleNode = BackupUtils.GetResponsibleNodeTag(Server.ServerStore, store.Database, periodicBackupTaskId);
                        msg += $" ResponsibleNode: '{responsibleNode ?? "null"}'.";

                        if (getStatusResult != null)
                        {
                            var bjro = context.ReadObject(getStatusResult.ToJson(), "status");
                            msg += $" {nameof(PeriodicBackupStatus)}:{Environment.NewLine}{bjro}";
                        }
                        else
                            msg += $" {nameof(PeriodicBackupStatus)} is null.";
                    }

                    return msg;
                }
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
                var responsibleDatabase = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
                Assert.NotNull(responsibleDatabase);
                Backup.WaitForResponsibleNodeUpdate(server.ServerStore, store.Database, taskId);

                var tag = responsibleDatabase.PeriodicBackupRunner.WhoseTaskIsIt(taskId);
                Assert.Equal(server.ServerStore.NodeTag, tag);

                responsibleDatabase.PeriodicBackupRunner.ForTestingPurposesOnly().SimulateActiveByOtherNodeStatus_Reschedule = true;
                var pb = responsibleDatabase.PeriodicBackupRunner.PeriodicBackups.First();
                Assert.NotNull(pb);

                var val = WaitForValue(() => pb.HasScheduledBackup(), false, timeout: 66666, interval: 444);
                Assert.False(val, "PeriodicBackup should cancel the ScheduledBackup if the task status is ActiveByOtherNode, " +
                                  "so when the task status is back to be ActiveByCurrentNode, UpdateConfigurations will be able to reassign the backup timer");

                responsibleDatabase.PeriodicBackupRunner._forTestingPurposes = null;
                responsibleDatabase.PeriodicBackupRunner.UpdateConfigurations(record1.PeriodicBackups);
                var getPeriodicBackupStatus = new GetPeriodicBackupStatusOperation(taskId);

                val = WaitForValue(() => store.Maintenance.Send(getPeriodicBackupStatus).Status?.LastFullBackup != null, true, timeout: 66666, interval: 444);
                Assert.True(val, "Failed to complete the backup in time");
            }
        }

        [RavenFact(RavenTestCategory.BackupExportImport | RavenTestCategory.Cluster)]
        public async Task ServerWideBackup_WithPinnedMentorNode_FailureHandling()
        {
            const int clusterSize = 3;

            var backupPath = NewDataPath(suffix: "BackupFolder");
            (List<RavenServer> nodes, RavenServer leaderServer) = await CreateRaftCluster(clusterSize);
            var mentorNode = nodes.First(x => x != leaderServer);

            using (var store = GetDocumentStore(new Options { Server = leaderServer, ReplicationFactor = 3}))
            {
                await Backup.FillClusterDatabaseWithRandomDataAsync(databaseSizeInMb: 1, store, clusterSize);

                var result = await store.Maintenance.Server.SendAsync(new PutServerWideBackupConfigurationOperation(new ServerWideBackupConfiguration
                {
                    FullBackupFrequency = "* * * * *", // We will perform the backup every minute
                    LocalSettings = new LocalSettings { FolderPath = backupPath },
                    MentorNode = mentorNode.ServerStore.NodeTag,
                    PinToMentorNode = true
                }));

                var serverWideConfiguration = await store.Maintenance.Server.SendAsync(new GetServerWideBackupConfigurationOperation(result.Name));
                var taskId = serverWideConfiguration.TaskId;

                var getPeriodicBackupStatusOperation = new GetPeriodicBackupStatusOperation(taskId);
                await WaitForValueAsync(async () =>
                {
                    var response = await store.Maintenance.SendAsync(getPeriodicBackupStatusOperation);
                    return response is { Status.LastFullBackup: not null };
                }, expectedVal: true);

                // Simulate mentor node failure
                await DisposeAndRemoveServer(mentorNode);

                // Wait a minute to ensure that no node has performed a backup according to the schedule (once a minute)
                await Task.Delay(TimeSpan.FromMinutes(1));
                foreach (var server in nodes.Where(node => node != mentorNode))
                {
                    var database = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database).ConfigureAwait(false);
                    Assert.NotNull(database);

                    var actualNodeTag = database.PeriodicBackupRunner.WhoseTaskIsIt(taskId);
                    Assert.Equal(mentorNode.ServerStore.NodeTag, actualNodeTag);

                    var backup = database.PeriodicBackupRunner.PeriodicBackups.SingleOrDefault();
                    Assert.True(backup != null,
                        $"Expected single backup task on Node '{server.ServerStore.NodeTag}' for database '{database.Name}' " +
                        $"Number of backup tasks: '{database.PeriodicBackupRunner.PeriodicBackups.Count}'.");

                    Assert.False(backup.HasScheduledBackup(),
                        $"Expected PeriodicBackup with pinned to mentor node '{mentorNode}' to cancel " +
                        $"the ScheduledBackup on Node '{server.ServerStore.NodeTag}', but it didn't.");

                    Assert.True(backup.BackupStatus == null,
                        $"Expected no backup on Node '{server.ServerStore.NodeTag}' as " +
                        $"PeriodicBackup is pinned to mentor node '{mentorNode}', but a backup was performed.");
                }
            }
        }

        // Performing backup Delay to the time:
        [RavenTheory(RavenTestCategory.BackupExportImport), Trait("Category", "Smuggler")]
        [InlineData(1)] // until the next scheduled backup time.
        [InlineData(5)] // after the next scheduled backup.
        public async Task ShouldProperlyPlaceOriginalBackupTimePropertyWithDelay(int delayDurationInMinutes)
        {
            const string fullBackupFrequency = "*/2 * * * *";
            var backupPath = NewDataPath(suffix: "BackupFolder");

            using (var server = GetNewServer())
            using (var store = GetDocumentStore(new Options { Server = server }))
            {
                using (var session = store.OpenAsyncSession())
                    await Backup.FillDatabaseWithRandomDataAsync(databaseSizeInMb: 1, session);

                var database = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
                Assert.NotNull(database);
                await Backup.HoldBackupExecutionIfNeededAndInvoke(database.PeriodicBackupRunner.ForTestingPurposesOnly(), async () =>
                {
                    WaitForValue(() =>
                    {
                        var now = DateTime.Now;
                        return now.Minute % 2 == 0 && now.Second <= 10;
                    },
                       expectedVal: true,
                       timeout: (int)TimeSpan.FromMinutes(2).TotalMilliseconds,
                       interval: (int)TimeSpan.FromSeconds(1).TotalMilliseconds
                   );

                    var config = Backup.CreateBackupConfiguration(backupPath, fullBackupFrequency: fullBackupFrequency);
                    var taskId = await Backup.UpdateConfigAndRunBackupAsync(server, config, store, opStatus: OperationStatus.InProgress);
                    // Let's delay the backup task
                    var taskBackupInfo = await store.Maintenance.SendAsync(new GetOngoingTaskInfoOperation(taskId, OngoingTaskType.Backup)) as OngoingTaskBackup;
                    Assert.NotNull(taskBackupInfo);
                    Assert.NotNull(taskBackupInfo.OnGoingBackup);
                    Assert.NotNull(taskBackupInfo.OnGoingBackup.StartTime);

                    var delayDuration = TimeSpan.FromMinutes(delayDurationInMinutes);
                    var delayUntil = DateTime.Now + delayDuration;
                    await store.Maintenance.SendAsync(new DelayBackupOperation(taskBackupInfo.OnGoingBackup.RunningBackupTaskId, delayDuration));

                    // There should be no OnGoingBackup operation in the OngoingTaskBackup
                    await WaitForValueAsync(async () =>
                    {
                        var afterDelayTaskBackupInfo = await store.Maintenance.SendAsync(new GetOngoingTaskInfoOperation(taskId, OngoingTaskType.Backup)) as OngoingTaskBackup;
                        return afterDelayTaskBackupInfo is { OnGoingBackup: null };
                    }, true);

                    var backupStatus = (await store.Maintenance.SendAsync(new GetPeriodicBackupStatusOperation(taskId))).Status;
                    Assert.NotNull(backupStatus);
                    Assert.NotNull(backupStatus.DelayUntil);
                    Assert.NotNull(backupStatus.OriginalBackupTime);

                    var nextFullBackup = GetNextBackupOccurrence(new NextBackupOccurrenceParameters
                    {
                        BackupFrequency = fullBackupFrequency,
                        Configuration = config,
                        LastBackupUtc = taskBackupInfo.OnGoingBackup.StartTime.Value
                    });
                    Assert.NotNull(nextFullBackup);

                    Assert.Equal(backupStatus.OriginalBackupTime,
                        delayUntil < nextFullBackup
                            ? taskBackupInfo.OnGoingBackup.StartTime    // until the next scheduled backup time.
                            : nextFullBackup.Value.ToUniversalTime());  // after the next scheduled backup.
                }, tcs: new TaskCompletionSource<object>(TaskCreationOptions.RunContinuationsAsynchronously));
            }
        }
    }
}
