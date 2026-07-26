using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Http;
using Raven.Client.ServerWide.Operations;
using Raven.Server;
using Raven.Server.Documents;
using Raven.Server.ServerWide.Backups;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;
using static Raven.Server.ServerWide.Backups.ServerBackupRunner;

namespace SlowTests.Server.Documents.PeriodicBackup
{
    public class BackupCancellationTests : RavenTestBase
    {
        public BackupCancellationTests(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.BackupExportImport)]
        public async Task Disable_CancelsInFlightBackup()
        {
            DoNotReuseServer();
            var backupPath = NewDataPath(suffix: "BackupFolder");

            using var server = GetNewServer();
            using var store = GetDocumentStore(new Options { Server = server });

            var db = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
            await StoreSomeDataAsync(store);

            var taskId = await Backup.UpdateConfigAsync(server, Backup.CreateBackupConfiguration(backupPath), store);

            var (tcs, state, opId) = StartPinnedBackup(server, db, taskId);

            // Disable the task. The record change cancels the in-flight backup (storage stays loaded).
            await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(
                Backup.CreateBackupConfiguration(backupPath, disabled: true, taskId: taskId)));

            Assert.True(WaitForDecisionLog(state, "[CANCELLED:disabled]"),
                "Expected a [CANCELLED:disabled] decision-log entry after the task was disabled.");

            tcs.SetResult(null); // release the pin so the backup observes the cancelled token

            await AssertOperationCanceledAsync(store, server, opId);
            await AssertNoBackupStatusRowAsync(store, taskId);
        }

        [RavenFact(RavenTestCategory.BackupExportImport)]
        public async Task DeleteTask_CancelsInFlightBackup()
        {
            DoNotReuseServer();
            var backupPath = NewDataPath(suffix: "BackupFolder");

            using var server = GetNewServer();
            using var store = GetDocumentStore(new Options { Server = server });

            var db = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
            await StoreSomeDataAsync(store);

            var taskId = await Backup.UpdateConfigAsync(server, Backup.CreateBackupConfiguration(backupPath), store);

            var (tcs, state, opId) = StartPinnedBackup(server, db, taskId);

            // Remove the task from the database record.
            await store.Maintenance.SendAsync(new DeleteOngoingTaskOperation(taskId, OngoingTaskType.Backup));

            Assert.True(WaitForDecisionLog(state, "[CANCELLED:task-deleted]"),
                "Expected a [CANCELLED:task-deleted] decision-log entry after the task was deleted.");

            tcs.SetResult(null);

            await AssertOperationCanceledAsync(store, server, opId);

            // The state is removed from the runner's per-task registry.
            Assert.True(WaitForValue(() => server.ServerStore.BackupRunner.GetDatabaseStateByTaskId(db.Name, taskId) == null,
                expectedVal: true, timeout: 30_000, interval: 200), "Expected the backup state to be removed from the runner registry.");

            // No orphan status row for the deleted task.
            await AssertNoBackupStatusRowAsync(store, taskId);
        }

        [RavenFact(RavenTestCategory.BackupExportImport)]
        public async Task DeleteDb_CancelsInFlightBackup()
        {
            DoNotReuseServer();
            var backupPath = NewDataPath(suffix: "BackupFolder");

            using var server = GetNewServer();
            using var store = GetDocumentStore(new Options { Server = server });

            var db = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
            await StoreSomeDataAsync(store);

            var taskId = await Backup.UpdateConfigAsync(server, Backup.CreateBackupConfiguration(backupPath), store);

            var (tcs, _, opId) = StartPinnedBackup(server, db, taskId);

            // Capture the operation reference before the database (and its Operations registry) is disposed.
            var operation = db.Operations.GetOperation(opId);
            Assert.NotNull(operation);

            // Delete the database in the background. DocumentDatabase.Dispose cancels the backup token via
            // DatabaseShutdown and then — in Operations.Dispose, which runs BEFORE DocumentsStorage.Dispose —
            // waits for the in-flight (killable) backup to wind down. So once the shutdown token is
            // cancelled we release the pin: the backup observes cancellation while storage is still alive
            // and ends cleanly Canceled (no ObjectDisposedException, no status row).
            var deleteSw = Stopwatch.StartNew();
            var deleteTask = store.Maintenance.Server.SendAsync(new DeleteDatabasesOperation(db.Name, hardDelete: true));

            Assert.True(WaitForValue(() => db.DatabaseShutdown.IsCancellationRequested, expectedVal: true, timeout: 30_000, interval: 100),
                "Database shutdown token was not cancelled after the delete was issued.");

            tcs.SetResult(null); // release the pin so the backup observes the cancelled token and winds down

            await deleteTask;
            deleteSw.Stop();
            Assert.True(deleteSw.Elapsed < TimeSpan.FromSeconds(30),
                $"Database delete took too long ({deleteSw.Elapsed}).");

            // The backup ends Canceled — not Completed (no successful upload/status), and not Faulted with
            // ObjectDisposedException (storage is still alive when the token is observed).
            var finalStatus = await WaitForOperationTerminalStatusAsync(operation);
            Assert.Equal(OperationStatus.Canceled, finalStatus);
        }

        [RavenFact(RavenTestCategory.BackupExportImport)]
        public async Task Disable_NoRunning_OnlyStale()
        {
            DoNotReuseServer();
            var backupPath = NewDataPath(suffix: "BackupFolder");

            using var server = GetNewServer();
            using var store = GetDocumentStore(new Options { Server = server });

            var db = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
            await StoreSomeDataAsync(store);

            var taskId = await Backup.UpdateConfigAsync(server, Backup.CreateBackupConfiguration(backupPath), store);

            var state = server.ServerStore.BackupRunner.GetDatabaseStateByTaskId(db.Name, taskId);
            Assert.NotNull(state);
            Assert.Null(state.RunningTask); // no backup is running

            // Disable the task while nothing is running.
            await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(
                Backup.CreateBackupConfiguration(backupPath, disabled: true, taskId: taskId)));

            // Stale should be raised, but no cancellation should be attempted and no [CANCELLED] entry written.
            Assert.True(WaitForValue(() => state.Stale.IsRaised(), expectedVal: true, timeout: 30_000, interval: 200),
                "Expected Stale to be raised after disabling the task.");

            // Give the runner a moment to process the change, then confirm no cancellation marker appeared.
            Assert.False(WaitForDecisionLog(state, "[CANCELLED", timeout: 3_000),
                "Disabling a task with no running backup must not write a [CANCELLED] decision-log entry.");
            Assert.Null(state.RunningTask);
        }

        [RavenFact(RavenTestCategory.BackupExportImport)]
        public async Task ConcurrentTriggers_NoCrash()
        {
            DoNotReuseServer();
            var backupPath = NewDataPath(suffix: "BackupFolder");

            using var server = GetNewServer();
            using var store = GetDocumentStore(new Options { Server = server });

            var db = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
            await StoreSomeDataAsync(store);

            var taskId = await Backup.UpdateConfigAsync(server, Backup.CreateBackupConfiguration(backupPath), store);

            var (tcs, _, opId) = StartPinnedBackup(server, db, taskId);
            var operation = db.Operations.GetOperation(opId);
            Assert.NotNull(operation);

            var unobserved = 0;
            void OnUnobserved(object _, UnobservedTaskExceptionEventArgs e)
            {
                Interlocked.Increment(ref unobserved);
                e.SetObserved();
            }

            TaskScheduler.UnobservedTaskException += OnUnobserved;
            try
            {
                // Fire disable AND db-delete in quick succession against the same in-flight backup.
                var disableTask = store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(
                    Backup.CreateBackupConfiguration(backupPath, disabled: true, taskId: taskId)));
                var deleteTask = store.Maintenance.Server.SendAsync(new DeleteDatabasesOperation(db.Name, hardDelete: true));

                // Once the database starts shutting down, release the pin so the backup can wind down. Both
                // triggers cancel the same token; the try/catch in CancelRunningBackup absorbs any
                // disposed-CTS / double-cancel race.
                Assert.True(WaitForValue(() => db.DatabaseShutdown.IsCancellationRequested, expectedVal: true, timeout: 30_000, interval: 100),
                    "Database shutdown token was not cancelled after the concurrent triggers.");

                tcs.SetResult(null);

                // The disable maintenance call may fail if the database is already being deleted — that is
                // fine; we only care that nothing crashed (no NRE, no double-dispose throw).
                try { await disableTask; } catch { /* db may be gone */ }
                await deleteTask;

                var finalStatus = await WaitForOperationTerminalStatusAsync(operation);
                Assert.NotEqual(OperationStatus.Completed, finalStatus);

                // Force a GC so any unobserved task faults surface through the finalizer.
                GC.Collect();
                GC.WaitForPendingFinalizers();
                GC.Collect();

                Assert.Equal(0, unobserved);
            }
            finally
            {
                TaskScheduler.UnobservedTaskException -= OnUnobserved;
            }
        }

        [RavenFact(RavenTestCategory.BackupExportImport)]
        public async Task ValueChange_AfterDatabaseRemoved_DoesNotThrow()
        {
            DoNotReuseServer();
            var backupPath = NewDataPath(suffix: "BackupFolder");

            using var server = GetNewServer();
            using var store = GetDocumentStore(new Options { Server = server });

            var db = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
            await StoreSomeDataAsync(store);

            await Backup.UpdateConfigAsync(server, Backup.CreateBackupConfiguration(backupPath), store);
            var databaseRecord = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));

            var runner = server.ServerStore.BackupRunner;

            runner.RemoveDatabase(db.Name);

            var ex = Record.Exception(() => runner.HandleDatabaseRecordChange(databaseRecord));
            Assert.Null(ex);
            Assert.Empty(runner.GetDatabaseBackups(db.Name));
        }

        [RavenFact(RavenTestCategory.BackupExportImport)]
        public async Task Resumability_CancelledBackupReRunsAsFull()
        {
            DoNotReuseServer();
            var backupPath = NewDataPath(suffix: "BackupFolder");

            using var server = GetNewServer();
            using var store = GetDocumentStore(new Options { Server = server });

            var db = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
            await StoreSomeDataAsync(store);

            var taskId = await Backup.UpdateConfigAsync(server, Backup.CreateBackupConfiguration(backupPath), store);

            var (tcs, state, opId) = StartPinnedBackup(server, db, taskId);

            // Cancel the full backup via disable.
            await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(
                Backup.CreateBackupConfiguration(backupPath, disabled: true, taskId: taskId)));
            Assert.True(WaitForDecisionLog(state, "[CANCELLED:disabled]"));
            tcs.SetResult(null);
            await AssertOperationCanceledAsync(store, server, opId);

            // The cancelled run must not have advanced LastFullBackup.
            await AssertNoBackupStatusRowAsync(store, taskId);

            // Drop the pin hook so the next backup runs to completion.
            server.ServerStore.BackupRunner.ForTestingPurposesOnly().DatabaseTestingStuffInternals.TryRemove(db.Name, out _);

            // Re-enable the task and run a backup; it must perform a full backup (LastFullBackup was never set).
            await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(
                Backup.CreateBackupConfiguration(backupPath, disabled: false, taskId: taskId)));
            Backup.WaitForResponsibleNodeUpdate(server.ServerStore, store.Database, taskId);

            var status = await Backup.RunBackupAndReturnStatusAsync(server, taskId, store, isFullBackup: true);
            Assert.NotNull(status);
            Assert.NotNull(status.LastFullBackup); // a full backup was performed after re-enabling
        }

        [RavenFact(RavenTestCategory.BackupExportImport)]
        public async Task FinishBackup_StaleBackup_NoNextBackupRecompute()
        {
            DoNotReuseServer();
            var backupPath = NewDataPath(suffix: "BackupFolder");

            using var server = GetNewServer();
            using var store = GetDocumentStore(new Options { Server = server });

            var db = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
            await StoreSomeDataAsync(store);

            var taskId = await Backup.UpdateConfigAsync(server, Backup.CreateBackupConfiguration(backupPath), store);

            var (tcs, state, opId) = StartPinnedBackup(server, db, taskId);

            var unobserved = 0;
            void OnUnobserved(object _, UnobservedTaskExceptionEventArgs e)
            {
                Interlocked.Increment(ref unobserved);
                e.SetObserved();
            }

            TaskScheduler.UnobservedTaskException += OnUnobserved;
            try
            {
                // Delete the task to raise Stale before the backup's FinishBackup continuation runs.
                await store.Maintenance.SendAsync(new DeleteOngoingTaskOperation(taskId, OngoingTaskType.Backup));
                Assert.True(WaitForDecisionLog(state, "[CANCELLED:task-deleted]"));

                tcs.SetResult(null);

                await AssertOperationCanceledAsync(store, server, opId);

                // FinishBackup ran with Stale raised, so it must have set NextBackup = null instead of
                // recomputing it (no GetNextBackupDetails call, no exception in the continuation).
                Assert.True(WaitForValue(() => state.NextBackup == null, expectedVal: true, timeout: 30_000, interval: 100),
                    "Expected NextBackup to be null after FinishBackup ran for a Stale state.");

                GC.Collect();
                GC.WaitForPendingFinalizers();
                GC.Collect();

                Assert.Equal(0, unobserved);
            }
            finally
            {
                TaskScheduler.UnobservedTaskException -= OnUnobserved;
            }
        }

        private static async Task StoreSomeDataAsync(IDocumentStore store)
        {
            // A few hundred documents guarantee the smuggler reaches a token check point when it runs with
            // an already-cancelled token, so the cancellation is observed deterministically.
            using var session = store.OpenAsyncSession();
            for (var i = 0; i < 500; i++)
                await session.StoreAsync(new Item { Data = new string('x', 256) }, $"items/{i}");
            await session.SaveChangesAsync();
        }

        private class Item
        {
            public string Data { get; set; }
        }

        private (TaskCompletionSource<object> tcs, DatabaseBackupState state, long opId) StartPinnedBackup(
            RavenServer server, DocumentDatabase db, long taskId)
        {
            var tcs = new TaskCompletionSource<object>(TaskCreationOptions.RunContinuationsAsynchronously);
            server.ServerStore.BackupRunner.ForTestingPurposesOnly().DatabaseTestingStuffInternals[db.Name] =
                new TestingStuffInternal { OnBackupTaskRunHoldBackupExecution = tcs };

            var opId = db.Operations.GetNextOperationId();
            server.ServerStore.BackupRunner.StartBackupTask(db.Name, taskId, isFullBackup: true, opId);

            var state = server.ServerStore.BackupRunner.GetDatabaseStateByTaskId(db.Name, taskId);
            Assert.NotNull(state);

            // RunningTask is set right before the pin in BackupTask.Run, so this confirms the backup is
            // genuinely in flight and suspended at the hold.
            Assert.True(WaitForValue(() => state.RunningTask != null, expectedVal: true, timeout: 30_000, interval: 100),
                "Backup did not reach the in-flight hold within the timeout.");

            return (tcs, state, opId);
        }

        private static bool WaitForDecisionLog(DatabaseBackupState state, string marker, int timeout = 30_000)
        {
            return WaitForValue(() => state.GetDecisionLog().Any(e => e.Reason.Contains(marker)),
                expectedVal: true, timeout: timeout, interval: 100);
        }

        private async Task AssertOperationCanceledAsync(IDocumentStore store, RavenServer server, long opId)
        {
            RavenCommand<OperationState> command = null;
            var reached = await WaitForValueAsync(async () =>
            {
                command = await Backup.ExecuteGetOperationStateCommand(store, opId, server.ServerStore.NodeTag);
                return command.Result != null && command.Result.Status == OperationStatus.Canceled;
            }, expectedVal: true, timeout: 30_000, interval: 200);

            Assert.True(reached, $"Operation {opId} was not observed as Canceled; last status: {command?.Result?.Status.ToString() ?? "null"}.");
        }

        private async Task AssertNoBackupStatusRowAsync(IDocumentStore store, long taskId)
        {
            var status = (await store.Maintenance.SendAsync(new GetPeriodicBackupStatusOperation(taskId))).Status;
            Assert.True(status == null || status.LastFullBackup == null,
                "A cancelled backup must not write a PeriodicBackupStatus row advancing LastFullBackup.");
        }

        private static async Task<OperationStatus> WaitForOperationTerminalStatusAsync(
            Raven.Server.Documents.Operations.AbstractOperation operation, int timeout = 30_000)
        {
            var sw = Stopwatch.StartNew();
            while (sw.ElapsedMilliseconds < timeout)
            {
                var status = operation.State?.Status;
                if (status is OperationStatus.Completed or OperationStatus.Faulted or OperationStatus.Canceled)
                    return status.Value;
                await Task.Delay(100);
            }

            return operation.State?.Status ?? OperationStatus.InProgress;
        }
    }
}
