using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using FastTests.Utils;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.CompareExchange;
using Raven.Client.Http;
using Raven.Server;
using Raven.Server.Config;
using Raven.Server.Documents.PeriodicBackup;
using Raven.Server.ServerWide.Context;
using Tests.Infrastructure;
using Xunit;

namespace StressTests.Server.Documents.PeriodicBackup
{
    public class RavenDB_11139_Stress : ClusterTestBase
    {
        private readonly ITestOutputHelper _output;

        public RavenDB_11139_Stress(ITestOutputHelper output) : base(output)
        {
            _output = output;
        }

        [RavenTheory(RavenTestCategory.BackupExportImport | RavenTestCategory.CompareExchange)]
        [RavenData(DatabaseMode = RavenDatabaseMode.Single)]
        public async Task CompareExchangeTombstoneCleaner_ShouldWorkProperly_WithCluster_WithMentorNodeChanges(Options options)
        {
            const int clusterSize = 3;

            var backupPath = NewDataPath(suffix: "BackupFolder");
            var databaseName = GetDatabaseName();
            var customSettings = new Dictionary<string, string> { { RavenConfiguration.GetKey(x => x.Cluster.CompareExchangeTombstonesCleanupInterval), "100" } };

            (List <RavenServer> clusterNodes, RavenServer leaderNode) = await CreateRaftCluster(clusterSize, customSettings: customSettings);
            var diagnosticLogBuilder = new StringBuilder();
            foreach (var node in clusterNodes.Where(node => node.ServerStore.Observer is not null))
            {
                node.ServerStore.Observer.ForTestingPurposesOnly().OnDiagnosticLog += logLine => diagnosticLogBuilder.AppendLine($"[{DateTime.UtcNow:O}][Node {node.ServerStore.NodeTag}] {logLine}");
                node.ServerStore.ForTestingPurposesOnly().IgnoreClusterTransactionIndexInCompareExchangeCleaner = true;
                Cluster.WaitForFirstCompareExchangeTombstonesClean(node);
            }

            await CreateDatabaseInCluster(databaseName, clusterSize, leaderNode.WebUrl);
            var secondNode = clusterNodes.First(x => x != leaderNode);

            options.Server = leaderNode;
            options.ModifyDocumentStore = s => s.Conventions = new DocumentConventions { DisableTopologyUpdates = true };
            options.ModifyDatabaseName = _ => databaseName;
            options.CreateDatabase = false;
            using var firstStore = GetDocumentStore(options);

            options.Server = secondNode;
            using var secondStore = GetDocumentStore(options);

            var backupConfiguration = Backup.CreateBackupConfiguration(backupPath, fullBackupFrequency: "*/3 * * * *", incrementalBackupFrequency: "* * * * *", mentorNode: leaderNode.ServerStore.NodeTag, disabled: true);

            // Since the removal of CompareExchangeTombstones is tied to the backup schedule, it is critical that we start the test when a full backup has been
            // completed on schedule. If a full backup runs every three minutes and the test starts, for example, at 10:01, we will wait until 10:02 and then
            // proceed to create a periodic backup task. This way Full backup will start at 10:03
            await Backup.WaitUntilNextFullBackupActionWindowAsync(backupConfiguration, actionWindow: TimeSpan.FromSeconds(30), leaderNode.ServerStore.ServerShutdown, diagnosticLogBuilder);

            var nextBackupWaiter = new NextBackupWaiter(clusterTestBase: this)
                .WithDatabase(databaseName)
                .WithBackupConfiguration(backupConfiguration)
                .WithClusterNodes(clusterNodes)
                .WithClusterObserverConfirmation()
                .WithDiagnosticLog(diagnosticLogBuilder);

            // Schedule     | First Node  | Second Node |
            // -------------|-------------|-------------|
            // Full         | Full        | -           | <- we are here
            // Incremental  | -           | Full        |
            // Incremental  | -           | Incremental |
            // Full         | Full        | Full        |
            await nextBackupWaiter
                .SetMentorNodeTo(leaderNode, firstStore)
                .Expect(BackupKind.Full)
                .WaitNextOccurrenceAsync();

            await CreateCompareExchangeTombstone(firstStore, "cx/3");

            await CompareExchangeTombstoneCleanerTestHelper.Clean(clusterNodes, databaseName, ignoreClustrTrx: true);

            AssertCompareExchangeCounts(leaderNode, firstStore.Database, expectedTombstonesNumber: 1, expectedCompareExchangeNumber: 0, "After the first full backup and tombstone creation", diagnosticLogBuilder);
            AssertCompareExchangeCounts(secondNode, secondStore.Database, expectedTombstonesNumber: 1, expectedCompareExchangeNumber: 0, "After the first full backup and tombstone creation", diagnosticLogBuilder);

            // Schedule     | First Node  | Second Node |
            // -------------|-------------|-------------|
            // Full         | Full        | -           |
            // Incremental  | -           | Full        | <- we are here
            // Incremental  | -           | Incremental |
            // Full         | Full        | Full        |
            await nextBackupWaiter
                .SetMentorNodeTo(secondNode, secondStore)
                .Expect(BackupKind.Full)
                .WaitNextOccurrenceAsync();

            await CreateCompareExchangeTombstone(firstStore, "cx/4");

            await CompareExchangeTombstoneCleanerTestHelper.Clean(clusterNodes, databaseName, ignoreClustrTrx: true);

            AssertCompareExchangeCounts(leaderNode, firstStore.Database, expectedTombstonesNumber: 2, expectedCompareExchangeNumber: 0, "After the first incremental backup with full backup on the second node and tombstone creation", diagnosticLogBuilder);
            AssertCompareExchangeCounts(secondNode, secondStore.Database, expectedTombstonesNumber: 2, expectedCompareExchangeNumber: 0, "After the first incremental backup with full backup on the second node and tombstone creation", diagnosticLogBuilder);

            // Schedule     | First Node  | Second Node |
            // -------------|-------------|-------------|
            // Full         | Full        | -           |
            // Incremental  | -           | Full        |
            // Incremental  | -           | Incremental | <- we are here
            // Full         | Full        | Full        |
            await nextBackupWaiter
                .Expect(BackupKind.Incremental)
                .WaitNextOccurrenceAsync();

            await CreateCompareExchangeTombstone(firstStore, "cx/5");

            await CompareExchangeTombstoneCleanerTestHelper.Clean(clusterNodes, databaseName, ignoreClustrTrx: true);

            // The first node did not perform incremental backup at this point, so GetNextBackupDetails will return Incremental backup. In this case we cannot delete tombstones, as they are needed for incremental backup.
            AssertCompareExchangeCounts(leaderNode, firstStore.Database, expectedTombstonesNumber: 3, expectedCompareExchangeNumber: 0, "After the first incremental backup on the second node and tombstone creation", diagnosticLogBuilder);
            AssertCompareExchangeCounts(secondNode, secondStore.Database, expectedTombstonesNumber: 3, expectedCompareExchangeNumber: 0, "After the first incremental backup on the second node and tombstone creation", diagnosticLogBuilder);

            // After we returned the mentor node to the first node, the backup runner will detect that there is a missed incremental backup and will run it. It is a known issue that will be resolved in RavenDB-19958
            // We need to wait first that missed incremental backup to be run on the first node.
            await nextBackupWaiter
                .SetMentorNodeTo(leaderNode, firstStore)
                .Expect(BackupKind.Incremental)
                .WaitNextOccurrenceAsync();

            // Schedule     | First Node  | Second Node |
            // -------------|-------------|-------------|
            // Full         | Full        | -           |
            // Incremental  | -           | Full        |
            // Incremental  | -           | Incremental |
            // Full         | Full        | -           | <- we are here
            await nextBackupWaiter
                .SetMentorNodeTo(leaderNode, firstStore)
                .Expect(BackupKind.Full)
                .WaitNextOccurrenceAsync();

            await CreateCompareExchangeTombstone(firstStore, "cx/6");

            await CompareExchangeTombstoneCleanerTestHelper.Clean(clusterNodes, databaseName, ignoreClustrTrx: true);

            AssertCompareExchangeCounts(leaderNode, firstStore.Database, expectedTombstonesNumber: 1, expectedCompareExchangeNumber: 0, "After the scheduled full backup on both nodes and tombstone creation", diagnosticLogBuilder);
            AssertCompareExchangeCounts(secondNode, secondStore.Database, expectedTombstonesNumber: 1, expectedCompareExchangeNumber: 0, "After the scheduled full backup on both nodes and tombstone creation", diagnosticLogBuilder);
        }

        #region Helpers

        private class NextBackupWaiter
        {
            private DocumentStore _store;
            private List<RavenServer> _clusterNodes;
            private RavenServer _runningOnServer;
            private string _databaseName;
            private PeriodicBackupConfiguration _backupConfiguration;
            private bool _shouldWaitClusterObserverConfirmation = false;
            private BackupKind _expectedBackupKind;
            private StringBuilder _diagnosticLogBuilder;
            private readonly ClusterTestBase _parent;

            public NextBackupWaiter(ClusterTestBase clusterTestBase)
            {
                _parent = clusterTestBase;
            }

            public NextBackupWaiter WithClusterNodes(List<RavenServer> nodes)
            {
                _clusterNodes = nodes;
                return this;
            }

            public NextBackupWaiter WithDatabase(string databaseName)
            {
                _databaseName = databaseName;
                return this;
            }

            public NextBackupWaiter WithBackupConfiguration(PeriodicBackupConfiguration backupConfiguration)
            {
                _backupConfiguration = backupConfiguration;
                return this;
            }

            public NextBackupWaiter WithClusterObserverConfirmation()
            {
                _shouldWaitClusterObserverConfirmation = true;
                return this;
            }

            public NextBackupWaiter WithoutClusterObserverConfirmation()
            {
                _shouldWaitClusterObserverConfirmation = false;
                return this;
            }

            public NextBackupWaiter SetMentorNodeTo(RavenServer server, DocumentStore store)
            {
                _runningOnServer = server;
                _store = store;
                return this;
            }

            public NextBackupWaiter Expect(BackupKind backupKind)
            {
                _expectedBackupKind = backupKind;
                return this;
            }

            public NextBackupWaiter WithDiagnosticLog(StringBuilder diagnosticLogBuilder)
            {
                _diagnosticLogBuilder = diagnosticLogBuilder;
                return this;
            }

            public async Task WaitNextOccurrenceAsync(Func<Task<OperationStatus>> manualTrigger = null)
            {
                _diagnosticLogBuilder?.AppendLine($"[{DateTime.UtcNow:O}][Node {_runningOnServer.ServerStore.NodeTag}] --- Entering {nameof(WaitNextOccurrenceAsync)} for backup task with ID `{_backupConfiguration.TaskId}` on database `{_databaseName}`.");

                Assert.True(_store != null, "DocumentStore must be set before waiting for the next backup.");
                Assert.True(_clusterNodes is { Count: > 0 }, "Cluster nodes must be set before waiting for the next backup.");
                Assert.True(_runningOnServer != null, "Running server must be set before waiting for the next backup.");
                Assert.False(string.IsNullOrEmpty(_databaseName), "Database name must be set before waiting for the next backup.");

                (long operationId, var operationStatus) = await WaitForNextBackupOccurrenceAsync(manualTrigger);
                await WaitForFinishedBackupLocallyAsync(operationId, operationStatus);

                if (_shouldWaitClusterObserverConfirmation == false)
                    return;

                var status = await GetPeriodicBackupStatusAsync();
                await WaitClusterObservationConfirmation(_clusterNodes, status, _store);
                _diagnosticLogBuilder?.AppendLine($"[{DateTime.UtcNow:O}][Node {_runningOnServer.ServerStore.NodeTag}] Cluster observer confirmed the backup task with ID `{_backupConfiguration.TaskId}` on database `{_databaseName}`.");
            }

            public async Task TriggerNextOccurenceNowAsync(BackupKind backupKind)
            {
                Expect(backupKind);
                await WaitNextOccurrenceAsync(async () =>
                {
                    await _parent.Backup.RunBackupAsync(_runningOnServer, _backupConfiguration.TaskId, _store, isFullBackup: backupKind == BackupKind.Full, opStatus: OperationStatus.InProgress);
                    return OperationStatus.Completed;
                });
            }

            public async Task TriggerNextFaultedOccurenceNowAsync(BackupKind backupKind)
            {
                Expect(backupKind);

                var database = await _parent.GetDatabase(_store.Database, _runningOnServer);
                database.PeriodicBackupRunner.ForTestingPurposesOnly().SimulateFailedBackup = true;

                try
                {
                    await WaitNextOccurrenceAsync(async () =>
                    {
                        await _parent.Backup.RunBackupAsync(_runningOnServer, _backupConfiguration.TaskId, _store, isFullBackup: backupKind == BackupKind.Full, opStatus: OperationStatus.InProgress);
                        return OperationStatus.Faulted;
                    });
                }
                finally
                {
                    database.PeriodicBackupRunner.ForTestingPurposesOnly().SimulateFailedBackup = false;
                }
            }

            private async Task ChangeMentorNodeIfNeededAsync()
            {
                var database = await _parent.GetDatabase(_databaseName, _runningOnServer);
                Assert.NotNull(database);

                var periodicBackupConfiguration = database.ReadDatabaseRecord().PeriodicBackups.SingleOrDefault(x => x.TaskId == _backupConfiguration.TaskId);
                if (periodicBackupConfiguration == null)
                {
                    _backupConfiguration.TaskId = await _parent.Backup.UpdateConfigAsync(_runningOnServer, _backupConfiguration, _store);
                }
                else if (periodicBackupConfiguration.MentorNode == _runningOnServer.ServerStore.NodeTag &&
                          periodicBackupConfiguration.Disabled == false)
                {
                    _diagnosticLogBuilder?.AppendLine($"[{DateTime.UtcNow:O}][Node {_runningOnServer.ServerStore.NodeTag}] Mentor node for backup task with ID `{_backupConfiguration.TaskId}` is enabled and already set to `{_runningOnServer.ServerStore.NodeTag}`. No need to change it.");
                    return;
                }

                _backupConfiguration.MentorNode = _runningOnServer.ServerStore.NodeTag;
                _backupConfiguration.Disabled = false;
                await _parent.Backup.UpdateConfigAsync(_runningOnServer, _backupConfiguration, _store);

                _diagnosticLogBuilder?.AppendLine($"[{DateTime.UtcNow:O}][Node {_runningOnServer.ServerStore.NodeTag}] Setting mentor node for backup task with ID `{_backupConfiguration.TaskId}` to `{_runningOnServer.ServerStore.NodeTag}`.");

                await WaitAndAssertForValueAsync(() =>
                    {
                        var record = database.ReadDatabaseRecord();
                        if (record == null)
                            return Task.FromResult<string>(null);

                        var config = record.PeriodicBackups.SingleOrDefault(x => x.TaskId == _backupConfiguration.TaskId);
                        return Task.FromResult(config?.MentorNode);
                    },
                    expectedVal: _runningOnServer.ServerStore.NodeTag,
                    interval: (int)TimeSpan.FromSeconds(1).TotalMilliseconds,
                    timeout: (int)TimeSpan.FromSeconds(30).TotalMilliseconds);

                _diagnosticLogBuilder?.AppendLine($"[{DateTime.UtcNow:O}][Node {_runningOnServer.ServerStore.NodeTag}] Updated backup task with ID `{_backupConfiguration.TaskId}` to have mentor node `{_runningOnServer.ServerStore.NodeTag}`.");
            }

            private async Task<(long, OperationStatus)> WaitForNextBackupOccurrenceAsync(Func<Task<OperationStatus>> manualTrigger = null)
            {
                long operationId = 0;
                OperationStatus operationStatus = OperationStatus.Completed;

                var database = await _parent.GetDatabase(_databaseName, _runningOnServer);
                Assert.NotNull(database);

                await _parent.Backup.HoldBackupExecutionIfNeededAndInvoke(database.PeriodicBackupRunner.ForTestingPurposesOnly(), async () =>
                {
                    await ChangeMentorNodeIfNeededAsync();

                    _diagnosticLogBuilder?.AppendLine($"[{DateTime.UtcNow:O}][Node {_runningOnServer.ServerStore.NodeTag}] Starting to wait for the next backup occurrence for task with ID `{_backupConfiguration.TaskId}` on database `{_databaseName}`.");

                    if (manualTrigger != null)
                    {
                        _diagnosticLogBuilder?.AppendLine($"[{DateTime.UtcNow:O}][Node {_runningOnServer.ServerStore.NodeTag}] Manually triggering backup for task with ID `{_backupConfiguration.TaskId}` on database `{_databaseName}`.");
                        operationStatus = await manualTrigger.Invoke();
                        _diagnosticLogBuilder?.AppendLine($"[{DateTime.UtcNow:O}][Node {_runningOnServer.ServerStore.NodeTag}] Manual trigger for backup operation returned status: {operationStatus}.");
                    }

                    var onGoingTaskInfo = await _parent.Backup.WaitForOnGoingBackupNotNullAsync(_store, _backupConfiguration.TaskId);
                    var nextOperationId = database.Operations.GetNextOperationId() - 1;
                    Assert.True(nextOperationId == onGoingTaskInfo.OnGoingBackup.RunningBackupTaskId, $"Expected a new backup task to be started, but the last ongoing task ID is still `{onGoingTaskInfo.OnGoingBackup.RunningBackupTaskId}`." +
                                                                                                      $"{Environment.NewLine}Diagnostic Info: {_diagnosticLogBuilder?.ToString() ?? "N/A"}");

                    operationId = onGoingTaskInfo.OnGoingBackup.RunningBackupTaskId;

                    _diagnosticLogBuilder?.AppendLine($"[{DateTime.UtcNow:O}][Node {_runningOnServer.ServerStore.NodeTag}] Backup operation with ID `{operationId}` started for task with ID `{_backupConfiguration.TaskId}` on database `{_databaseName}`.");

                    var ongoingBackupKind = onGoingTaskInfo.OnGoingBackup.IsFull ? BackupKind.Full : BackupKind.Incremental;
                    Assert.True(ongoingBackupKind == _expectedBackupKind, $"Expected the ongoing backup task to be a {_expectedBackupKind}, but it is {ongoingBackupKind}.{Environment.NewLine}Diagnostic Info: {_diagnosticLogBuilder?.ToString() ?? "N/A"}");
                }, tcs: new TaskCompletionSource<object>(TaskCreationOptions.RunContinuationsAsynchronously));

                return (operationId, operationStatus);
            }

            private async Task WaitForFinishedBackupLocallyAsync(long operationId, OperationStatus expectedOperationStatus, int timeout = 15_000, int interval = 1_000)
            {
                RavenCommand<OperationState> command = null;
                await WaitForValueAsync(async () =>
                {
                    command = await _parent.Backup.ExecuteGetOperationStateCommand(_store, operationId, _runningOnServer.ServerStore.NodeTag);
                    return command.Result?.Status == expectedOperationStatus &&
                           command.StatusCode == HttpStatusCode.OK;
                },
                    expectedVal: true,
                    timeout: timeout,
                    interval: interval);

                Assert.True(command.Result?.Status == expectedOperationStatus,
                    $"Expected the backup operation with ID `{operationId}` for task with ID `{_backupConfiguration.TaskId}` on database `{_databaseName}` " +
                    $"to be {expectedOperationStatus}, but {(command.Result == null ? "command.Result is null" : $"it is {command.Result.Status}")}." +
                    $"{Environment.NewLine}Diagnostic Info: {_diagnosticLogBuilder?.ToString() ?? "N/A"}");

                Assert.True(command.StatusCode == HttpStatusCode.OK,
                    $"Expected the backup operation with ID `{operationId}` for task with ID `{_backupConfiguration.TaskId}` on database `{_databaseName}` " +
                    $"to return status code {HttpStatusCode.OK}, but it is {command.StatusCode}.{Environment.NewLine}Diagnostic Info: {_diagnosticLogBuilder?.ToString() ?? "N/A"}");

                await _parent.Backup.CheckBackupOperationStatus(expectedOperationStatus, command, _store, _backupConfiguration.TaskId, operationId, periodicBackupRunner: null);
                Assert.True(expectedOperationStatus == command.Result.Status, $"Expected the backup operation with ID `{operationId}` for task with ID `{_backupConfiguration.TaskId}` on database `{_databaseName}` to be {expectedOperationStatus}, but it is {command.Result.Status}.{Environment.NewLine}Diagnostic Info: {_diagnosticLogBuilder?.ToString() ?? "N/A"}");
                _diagnosticLogBuilder?.AppendLine($"[{DateTime.UtcNow:O}][Node {_runningOnServer.ServerStore.NodeTag}] Backup operation with ID `{operationId}` {command.Result.Status} for task with ID `{_backupConfiguration.TaskId}` on database `{_databaseName}`.");
            }

            private async Task<PeriodicBackupStatus> GetPeriodicBackupStatusAsync()
            {
                PeriodicBackupStatus status = null;
                await WaitAndAssertForValueAsync(() =>
                    {
                        status = _runningOnServer.ServerStore.DatabaseInfoCache.BackupStatusStorage.GetBackupStatus(_databaseName, _backupConfiguration.TaskId);
                        return Task.FromResult(status != null);
                    }, expectedVal: true,
                    interval: (int)TimeSpan.FromSeconds(1).TotalMilliseconds,
                    timeout: (int)TimeSpan.FromSeconds(90).TotalMilliseconds);

                return status;
            }

            private async Task WaitClusterObservationConfirmation(List<RavenServer> clusterNodes, PeriodicBackupStatus status, DocumentStore store)
            {
                await _parent.Backup.WaitAndAssertForClusterObserverToGetUpdatedBackupStatusAsync(store.Database, status, clusterNodes);
            }
        }

        private static async Task CreateCompareExchangeTombstone(DocumentStore documentStore, string key)
        {
            var res = await documentStore.Operations.SendAsync(new PutCompareExchangeValueOperation<int>(key, 1, 0));
            await documentStore.Operations.SendAsync(new DeleteCompareExchangeValueOperation<int>(key, res.Index));
        }

        private void AssertCompareExchangeCounts(RavenServer server, string databaseName, long expectedTombstonesNumber, long expectedCompareExchangeNumber, string message = null, StringBuilder diagnosticLogBuilder = null)
        {
            using (server.ServerStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
            {
                var actualTombstonesNumber = WaitForValue(() =>
                    {
                        long value;
                        using (context.OpenReadTransaction())
                            value = server.ServerStore.Cluster.GetNumberOfCompareExchangeTombstones(context, databaseName);

                        return value;
                    },
                    expectedVal: expectedTombstonesNumber,
                    timeout: (int) TimeSpan.FromSeconds(10).TotalMilliseconds,
                    interval: (int) TimeSpan.FromMilliseconds(500).TotalMilliseconds);

                var actualCompareExchangeNumber = WaitForValue(() =>
                    {
                        long value;
                        using (context.OpenReadTransaction())
                            value = server.ServerStore.Cluster.GetNumberOfCompareExchange(context, databaseName);

                        return value;
                    },
                    expectedVal: expectedCompareExchangeNumber,
                    timeout: (int) TimeSpan.FromSeconds(10).TotalMilliseconds,
                    interval: (int) TimeSpan.FromMilliseconds(500).TotalMilliseconds);

                Assert.True(expectedTombstonesNumber == actualTombstonesNumber,
                    $"Tombstones check failed. Expected: {expectedTombstonesNumber}, Actual: {actualTombstonesNumber}. " +
                    $"Step: '{message ?? "N/A"}'{Environment.NewLine}Diagnostic Info: {diagnosticLogBuilder?.ToString() ?? "N/A"}");

                Assert.True(expectedCompareExchangeNumber == actualCompareExchangeNumber,
                    $"Values check failed. Expected: {expectedCompareExchangeNumber}, Actual: {actualCompareExchangeNumber}. " +
                    $"Step: '{message ?? "N/A"}' {Environment.NewLine}Diagnostic Info: {diagnosticLogBuilder?.ToString() ?? "N/A"}");

                diagnosticLogBuilder?.AppendLine($"[{DateTime.UtcNow:O}][Node {server.ServerStore.NodeTag}] On step '{message ?? "N/A"}': Tombstones: {actualTombstonesNumber}, Values: {actualCompareExchangeNumber}");
            }
        }

        #endregion
    }
}
