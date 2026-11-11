using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Exceptions.Database;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Client.ServerWide.Operations.Configuration;
using Raven.Server;
using Raven.Server.Config;
using Raven.Server.ServerWide.Backups;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.PeriodicBackup;

// Repros for the review findings on RavenDB-24994 (centralized backup runner).
// Every test asserts the expected behavior, so it fails while the defect exists.
public class ServerBackupRunnerReviewRepros : ClusterTestBase
{
    public ServerBackupRunnerReviewRepros(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.BackupExportImport)]
    public async Task Finding01_ScheduledFullBackupMustBeFullAndNotRepeatEveryTick()
    {
        var backupPath = NewDataPath(suffix: "BackupFolder");
        using var server = GetNewServer(new ServerCreationOptions
        {
            CustomSettings = new Dictionary<string, string>
            {
                [RavenConfiguration.GetKey(x => x.Backup.BackupRunnerFrequency)] = "1"
            }
        });
        using var store = GetDocumentStore(new Options { Server = server });
        await StoreDocument(store);

        // full backup every minute, incremental effectively never
        var config = Backup.CreateBackupConfiguration(backupPath, fullBackupFrequency: "* * * * *", incrementalBackupFrequency: "0 0 1 1 *");

        await WaitForSecondOfMinuteBelow(40);
        var taskId = await Backup.UpdateConfigAndRunBackupAsync(server, config, store, isFullBackup: true);
        var manualDoneUtc = DateTime.UtcNow;

        Assert.True(await WaitForValueAsync(() => Task.FromResult(Decisions(server, store.Database, taskId)
                .Any(d => d.Category == BackupDecisionCategory.Completed && d.LastTime > manualDoneUtc)), true, timeout: 90_000, interval: 250),
            "precondition: the scheduler should run the next backup at the minute boundary");

        await Task.Delay(TimeSpan.FromSeconds(15));

        var decisions = Decisions(server, store.Database, taskId);
        var scheduledStarts = decisions.Where(d => d.Category == BackupDecisionCategory.Started && d.LastTime > manualDoneUtc).OrderBy(d => d.LastTime).ToList();
        var status = (await store.Maintenance.SendAsync(new GetPeriodicBackupStatusOperation(taskId))).Status;
        var files = Directory.GetFiles(backupPath, "*", SearchOption.AllDirectories).GroupBy(Path.GetExtension).Select(g => $"{g.Key}={g.Count()}");

        Assert.True(status.IsFull && scheduledStarts.Count == 1,
            $"Expected one scheduled FULL backup at the minute boundary. Got {scheduledStarts.Count} scheduled start(s) within ~15s " +
            $"(first at {scheduledStarts.FirstOrDefault()?.LastTime:O}); last status IsFull={status.IsFull}, " +
            $"LastFullBackup={status.LastFullBackup:O}, LastIncrementalBackup={status.LastIncrementalBackup:O}. " +
            $"Files: {string.Join(", ", files)}{Environment.NewLine}{Dump(decisions)}");
    }

    [RavenFact(RavenTestCategory.BackupExportImport)]
    public async Task Finding02_ServerWideBackupAddedWhileDatabaseIsIdleMustRun()
    {
        var backupPath = NewDataPath(suffix: "BackupFolder");
        using var server = GetNewServer(new ServerCreationOptions
        {
            RunInMemory = false,
            CustomSettings = IdleSettings()
        });
        using var store = GetDocumentStore(new Options { Server = server, RunInMemory = false });
        await StoreDocument(store);

        server.ServerStore.DatabasesLandlord.ForTestingPurposesOnly().SkipIncreasingLastWorkTimeBasedOnDatabaseSize = true;
        Assert.True(WaitForValue(() => server.ServerStore.IdleDatabases.ContainsKey(store.Database), true, timeout: 90_000, interval: 500),
            "precondition: database should become idle");

        var put = await store.Maintenance.Server.SendAsync(new PutServerWideBackupConfigurationOperation(new ServerWideBackupConfiguration
        {
            FullBackupFrequency = "* * * * *",
            LocalSettings = new LocalSettings { FolderPath = backupPath }
        }));
        await server.ServerStore.Cluster.WaitForIndexNotification(put.RaftCommandIndex);

        var taskId = GetSingleBackupTaskId(server, store.Database);
        var responsibleNode = Backup.WaitForResponsibleNodeUpdate(server.ServerStore, store.Database, taskId);
        Assert.Equal(server.ServerStore.NodeTag, responsibleNode);
        Assert.True(server.ServerStore.IdleDatabases.ContainsKey(store.Database), "precondition: database should still be idle");

        var backedUp = await WaitForValueAsync(() => Task.FromResult(GetClusterBackupStatus(server, store.Database, taskId)?.LastFullBackup != null),
            true, timeout: 130_000, interval: 1000);

        var tracked = server.ServerStore.ServerBackupRunner.GetDatabaseStateByTaskId(store.Database, taskId) != null;
        Assert.True(backedUp,
            $"Server-wide backup task {taskId} (every minute, responsible node '{responsibleNode}' = this node) never ran for idle database '{store.Database}' within 130s. " +
            $"Runner tracks the task: {tracked}. Database still idle: {server.ServerStore.IdleDatabases.ContainsKey(store.Database)}.");
    }

    [RavenFact(RavenTestCategory.BackupExportImport)]
    public async Task Finding03_BackupNowMustReportStartFailure()
    {
        var backupPath = NewDataPath(suffix: "BackupFolder");
        using var server = GetNewServer(new ServerCreationOptions { RunInMemory = false });
        using var store = GetDocumentStore(new Options { Server = server, RunInMemory = false });
        await StoreDocument(store);
        var taskId = await Backup.UpdateConfigAsync(server, Backup.CreateBackupConfiguration(backupPath), store);

        var database = await Databases.GetDocumentDatabaseInstanceFor(server, store);
        await File.WriteAllTextAsync(database.Configuration.Core.DataDirectory.Combine("disable.tasks.marker").FullPath, string.Empty);
        Assert.True(server.ServerStore.DatabasesLandlord.UnloadDirectly(store.Database), "precondition: unload database");
        database = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
        Assert.True(database.DisableOngoingTasks, "precondition: disable.tasks.marker should disable ongoing tasks");

        Operation<StartBackupOperationResult> result = null;
        var error = await Record.ExceptionAsync(async () => result = await store.Maintenance.SendAsync(new StartBackupOperation(isFullBackup: true, taskId)));

        OperationState operationState = null;
        if (result != null)
        {
            await Task.Delay(TimeSpan.FromSeconds(3));
            operationState = await store.Maintenance.SendAsync(new GetOperationStateOperation(result.Id));
        }

        Assert.True(error != null,
            $"Backup Now on a database with disable.tasks.marker did not fail; it returned operation id {result?.Id}. " +
            $"Operation registered on the server: {operationState != null}.{Environment.NewLine}{Dump(Decisions(server, store.Database, taskId))}");
    }

    [RavenFact(RavenTestCategory.BackupExportImport)]
    public async Task Finding04_DisabledDatabaseMustNotGetFailedBackups()
    {
        var backupPath = NewDataPath(suffix: "BackupFolder");
        using var server = GetNewServer(new ServerCreationOptions
        {
            RunInMemory = false,
            CustomSettings = new Dictionary<string, string>
            {
                [RavenConfiguration.GetKey(x => x.Backup.BackupRunnerFrequency)] = "1"
            }
        });
        using var store = GetDocumentStore(new Options { Server = server, RunInMemory = false });
        await StoreDocument(store);

        await WaitForSecondOfMinuteBelow(40);
        var taskId = await Backup.UpdateConfigAsync(server, Backup.CreateBackupConfiguration(backupPath, fullBackupFrequency: "* * * * *"), store);

        await store.Maintenance.Server.SendAsync(new ToggleDatabasesStateOperation(store.Database, disable: true));
        Assert.False(WaitForValue(() => server.ServerStore.DatabasesLandlord.TryGetDatabaseIfLoaded(store.Database, out _), false, timeout: 15_000, interval: 100),
            "precondition: disabled database should be unloaded");
        var disabledAtUtc = DateTime.UtcNow;

        await WaitUntilNextMinuteBoundary(extra: TimeSpan.FromSeconds(15));

        var afterDisable = Decisions(server, store.Database, taskId).Where(d => d.LastTime >= disabledAtUtc).ToList();
        var attempts = afterDisable.Where(d => d.Category is BackupDecisionCategory.Started or BackupDecisionCategory.Failed).ToList();
        var localStatus = server.ServerStore.DatabaseInfoCache.BackupStatusStorage.GetBackupStatus(store.Database, taskId);

        Assert.True(attempts.Count == 0,
            $"The runner tried to back up disabled database '{store.Database}' ({attempts.Count} Started/Failed decisions after it was disabled). " +
            $"Local status error: {localStatus?.Error?.Exception?.Split(Environment.NewLine).FirstOrDefault() ?? "none"}{Environment.NewLine}{Dump(afterDisable)}");
    }

    [RavenFact(RavenTestCategory.BackupExportImport)]
    public async Task Finding05_BackupNowWhileScheduledBackupIsStartingMustNotBeSilentlyDropped()
    {
        var backupPath = NewDataPath(suffix: "BackupFolder");
        using var server = GetNewServer(new ServerCreationOptions { RunInMemory = false });
        using var store = GetDocumentStore(new Options { Server = server, RunInMemory = false });
        await StoreDocument(store);
        // Backup Now runs on the loaded database and registers before it returns, so it can no longer start on an unloaded database (that was the bug in finding 3);
        // the backup held while starting is now the scheduled one, so the task runs every minute and the database instance is taken before it's unloaded
        var taskId = await Backup.UpdateConfigAsync(server, Backup.CreateBackupConfiguration(backupPath, fullBackupFrequency: "* * * * *"), store);

        var runner = server.ServerStore.ServerBackupRunner;
        var landlord = server.ServerStore.DatabasesLandlord;
        var state = await WaitForStateAsync(runner, store.Database, taskId);
        Assert.NotNull(state);

        var database = await Databases.GetDocumentDatabaseInstanceFor(server, store);
        await WaitUntilNextMinuteBoundary(extra: TimeSpan.FromSeconds(10));
        Assert.False(WaitForValue(() => state.Running.IsRaised(), false, timeout: 60_000, interval: 100), "precondition: no backup is running");
        Assert.True(landlord.UnloadDirectly(store.Database), "precondition: unload database");

        // block database loading, so the first backup stays between Running.Raise() and the RunningTask assignment
        var drained = 0;
        while (landlord._databaseSemaphore.Wait(0))
            drained++;
        var originalLoadTimeout = landlord._concurrentDatabaseLoadTimeout;
        landlord._concurrentDatabaseLoadTimeout = TimeSpan.FromMinutes(2);
        var released = false;

        try
        {
            const long secondOperationId = 1_000_002;

            Assert.True(WaitForValue(() => state.Running.IsRaised(), true, timeout: 75_000, interval: 50), "precondition: first backup is marked as running");
            Assert.Null(state.RunningTask);

            var error = Record.Exception(() => runner.StartBackupNowTask(database, taskId, isFullBackup: true, secondOperationId));

            Release();

            Assert.False(WaitForValue(() => state.Running.IsRaised(), false, timeout: 60_000, interval: 100), "first backup should finish");
            var loadedDatabase = await landlord.TryGetOrCreateResourceStore(store.Database);
            var secondRegistered = loadedDatabase.Operations.GetOperation(secondOperationId) != null;

            Assert.True(error is BackupAlreadyRunningException,
                $"The second StartBackupTask call returned operation id {secondOperationId} without throwing (error: {error?.GetType().Name ?? "none"}); " +
                $"that operation was registered: {secondRegistered}.");
        }
        finally
        {
            Release();
        }

        void Release()
        {
            if (released)
                return;

            released = true;
            if (drained > 0)
                landlord._databaseSemaphore.Release(drained);
            landlord._concurrentDatabaseLoadTimeout = originalLoadTimeout;
        }
    }

    [RavenFact(RavenTestCategory.BackupExportImport | RavenTestCategory.Sharding)]
    public async Task Finding06_ShardsOfTheSameDatabaseMustNotBlockEachOtherOnTheConcurrencyLimit()
    {
        var backupPath = NewDataPath(suffix: "BackupFolder");
        using var server = GetNewServer(new ServerCreationOptions
        {
            CustomSettings = new Dictionary<string, string>
            {
                [RavenConfiguration.GetKey(x => x.Backup.BackupRunnerFrequency)] = "1",
                [RavenConfiguration.GetKey(x => x.Backup.MaxNumberOfConcurrentBackups)] = "1"
            }
        });
        using var store = Sharding.GetDocumentStore(new Options { Server = server });
        await StoreDocument(store);

        var shardNames = new List<string>();
        await foreach (var shard in Sharding.GetShardsDocumentDatabaseInstancesFor(store, new List<RavenServer> { server }))
            shardNames.Add(shard.Name);
        Assert.True(shardNames.Count > 1, "precondition: more than one shard on this node");

        var runner = server.ServerStore.ServerBackupRunner;
        var holds = new List<ManualResetEvent>();
        foreach (var shardName in shardNames)
        {
            var hold = new ManualResetEvent(false);
            holds.Add(hold);
            runner.ForTestingPurposesOnly().DatabaseTestingStuffInternals[shardName] = new ServerBackupRunner.TestingStuffInternal { HoldBackupFromFinishing = hold };
        }

        try
        {
            var startUtc = DateTime.UtcNow;
            var result = await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(Backup.CreateBackupConfiguration(backupPath, fullBackupFrequency: "* * * * *")));

            Assert.True(await WaitForValueAsync(() => Task.FromResult(StartedShards() > 0), true, timeout: 90_000, interval: 250),
                "precondition: at least one shard backup should start at the minute boundary");

            // the remaining shards of the same database are allowed to run concurrently, give them 10 runner ticks
            await Task.Delay(TimeSpan.FromSeconds(10));

            var started = StartedShards();
            var capRejections = runner.Decisions.GetDecisionLogDetails(new BackupDecisionLogFilter { Take = BackupDecisionTracker.MaxDecisionLogSize }, afterEtag: null).Entries.Count(d => d.Source == BackupDecisionSource.Server && d.Detail == "ConcurrentBackup" && d.LastTime >= startUtc);

            Assert.True(started == shardNames.Count,
                $"Only {started} of {shardNames.Count} shards of '{store.Database}' started; the server log has {capRejections} 'ConcurrentBackup' rejections. " +
                $"Running backups per counter: {server.ServerStore.ConcurrentBackupsCounter.CurrentNumberOfRunningBackups} (limit {server.ServerStore.ConcurrentBackupsCounter.MaxNumberOfConcurrentBackups}).");

            int StartedShards() => shardNames.Count(name => Decisions(server, name, result.TaskId).Any(d => d.Category == BackupDecisionCategory.Started && d.LastTime >= startUtc));
        }
        finally
        {
            foreach (var hold in holds)
                hold.Set();
        }
    }

    [RavenFact(RavenTestCategory.BackupExportImport)]
    public async Task Finding07_BackupDisabledWhileDatabaseIsIdleMustNotRunAfterWakeUp()
    {
        var backupPath = NewDataPath(suffix: "BackupFolder");
        using var server = GetNewServer(new ServerCreationOptions
        {
            RunInMemory = false,
            CustomSettings = new Dictionary<string, string>(IdleSettings()) { [RavenConfiguration.GetKey(x => x.Backup.KeepIdleDatabaseLoadedBeforeBackup)] = "0" }
        });
        using var store = GetDocumentStore(new Options { Server = server, RunInMemory = false });
        await StoreDocument(store);

        var put = await store.Maintenance.Server.SendAsync(new PutServerWideBackupConfigurationOperation(new ServerWideBackupConfiguration
        {
            FullBackupFrequency = "* * * * *",
            LocalSettings = new LocalSettings { FolderPath = backupPath }
        }));
        await server.ServerStore.Cluster.WaitForIndexNotification(put.RaftCommandIndex);
        var taskId = GetSingleBackupTaskId(server, store.Database);
        Backup.WaitForResponsibleNodeUpdate(server.ServerStore, store.Database, taskId);

        var runner = server.ServerStore.ServerBackupRunner;
        Assert.True(WaitForValue(() => runner.GetDatabaseStateByTaskId(store.Database, taskId) != null, true), "precondition: runner tracks the task");

        server.ServerStore.DatabasesLandlord.ForTestingPurposesOnly().SkipIncreasingLastWorkTimeBasedOnDatabaseSize = true;

        // the database goes idle between the per-minute backups, disable the task while it's idle and far enough from the next minute
        for (var attempt = 0; ; attempt++)
        {
            Assert.True(WaitForValue(() => server.ServerStore.IdleDatabases.ContainsKey(store.Database), true, timeout: 90_000, interval: 250),
                "precondition: database should become idle");

            if (DateTime.Now.Second <= 40)
                break;

            Assert.True(attempt < 3, "precondition: could not catch the idle window");
            await WaitUntilNextMinuteBoundary(extra: TimeSpan.FromSeconds(2));
        }

        var serverWide = await store.Maintenance.Server.SendAsync(new GetServerWideBackupConfigurationOperation(put.Name));
        serverWide.Disabled = true;
        var disable = await store.Maintenance.Server.SendAsync(new PutServerWideBackupConfigurationOperation(serverWide));
        await server.ServerStore.Cluster.WaitForIndexNotification(disable.RaftCommandIndex);
        var disabledAtUtc = DateTime.UtcNow;

        Assert.True(GetSingleBackupConfiguration(server, store.Database).Disabled, "precondition: the task is disabled in the database record");
        Assert.True(server.ServerStore.IdleDatabases.ContainsKey(store.Database), "precondition: database is still idle when the task is disabled");
        var runnerStillSeesEnabled = runner.GetDatabaseStateByTaskId(store.Database, taskId).Configuration.Disabled == false;

        await WaitUntilNextMinuteBoundary(extra: TimeSpan.FromSeconds(20));

        var afterDisable = Decisions(server, store.Database, taskId).Where(d => d.LastTime >= disabledAtUtc).ToList();
        var started = afterDisable.Count(d => d.Category == BackupDecisionCategory.Started);

        Assert.True(started == 0,
            $"The runner started {started} backup(s) of task {taskId} after it was disabled at {disabledAtUtc:O} while the database was idle " +
            $"(runner configuration still enabled right after the disable: {runnerStillSeesEnabled}).{Environment.NewLine}{Dump(afterDisable)}");
    }

    [RavenFact(RavenTestCategory.BackupExportImport)]
    public async Task Finding08_DisableAndEnableWithinOneTickMustNotDuplicateQueueEntry()
    {
        using var server = GetNewServer(new ServerCreationOptions
        {
            CustomSettings = new Dictionary<string, string>
            {
                // no runner tick during the test
                [RavenConfiguration.GetKey(x => x.Backup.BackupRunnerFrequency)] = "3600"
            }
        });
        using var store = GetDocumentStore(new Options { Server = server });

        var config = Backup.CreateBackupConfiguration(NewDataPath(suffix: "BackupFolder"));
        var taskId = await Backup.UpdateConfigAsync(server, config, store);

        var runner = server.ServerStore.ServerBackupRunner;
        var state = await WaitForStateAsync(runner, store.Database, taskId);
        Assert.NotNull(state);
        Assert.Equal(1, CountInQueue(runner, state));

        config.TaskId = taskId;
        config.Disabled = true;
        await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config));
        Assert.True(WaitForValue(() => state.Stale.IsRaised(), true), "precondition: disabled task is stale");

        config.Disabled = false;
        await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config));
        Assert.False(WaitForValue(() => state.Stale.IsRaised(), false, timeout: 30_000), "precondition: re-enabled task is not stale");

        var count = CountInQueue(runner, state);
        // the debug endpoint that returned the queue info was removed; the decision log is now served over a WebSocket, so the numbers are read here
        var queue = (QueueLength: GetQueue(runner).Count, TrackedTasks: runner.BackupsPerDatabasePerTaskId.Sum(x => x.Value.Count));
        Assert.True(count == 1,
            $"Task {taskId} appears {count} times in the runner queue after disable+enable within one tick (QueueLength={queue.QueueLength}, TrackedTasks={queue.TrackedTasks}).");
    }

    [RavenFact(RavenTestCategory.BackupExportImport)]
    public async Task Finding09_OneTimeBackupMustRespectConcurrentBackupsLimit()
    {
        using var server = GetNewServer(new ServerCreationOptions
        {
            CustomSettings = new Dictionary<string, string>
            {
                [RavenConfiguration.GetKey(x => x.Backup.MaxNumberOfConcurrentBackups)] = "1"
            }
        });
        using var store1 = GetDocumentStore(new Options { Server = server });
        using var store2 = GetDocumentStore(new Options { Server = server });
        await StoreDocument(store1);
        await StoreDocument(store2);

        var taskId = await Backup.UpdateConfigAsync(server, Backup.CreateBackupConfiguration(NewDataPath(suffix: "BackupFolder1")), store1);

        var runner = server.ServerStore.ServerBackupRunner;
        var counter = server.ServerStore.ConcurrentBackupsCounter;
        var hold = new TaskCompletionSource<object>(TaskCreationOptions.RunContinuationsAsynchronously);
        runner.ForTestingPurposesOnly().DatabaseTestingStuffInternals[store1.Database] = new ServerBackupRunner.TestingStuffInternal { OnBackupTaskRunHoldBackupExecution = hold };

        try
        {
            var database1 = await Databases.GetDocumentDatabaseInstanceFor(server, store1);
            runner.StartBackupNowTask(database1, taskId, isFullBackup: true, database1.Operations.GetNextOperationId());
            Assert.Equal(1, counter.MaxNumberOfConcurrentBackups);
            Assert.Equal(1, WaitForValue(() => counter.CurrentNumberOfRunningBackups, 1));

            var error = await Record.ExceptionAsync(async () =>
            {
                var operation = await store2.Maintenance.SendAsync(new BackupOperation(new BackupConfiguration
                {
                    BackupType = BackupType.Backup,
                    LocalSettings = new LocalSettings { FolderPath = NewDataPath(suffix: "BackupFolder2") }
                }));
                await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));
            });

            Assert.True(error != null,
                $"A one-time backup of '{store2.Database}' completed while '{store1.Database}' held the only concurrent backup slot (limit {counter.MaxNumberOfConcurrentBackups}).");
        }
        finally
        {
            hold.TrySetResult(null);
        }
    }

    [RavenFact(RavenTestCategory.BackupExportImport)]
    public async Task Finding09_BackupNowMustRespectServerHealthChecks()
    {
        using var server = GetNewServer(new ServerCreationOptions { IgnoreHealthChecksBeforeBackup = false });
        using var store = GetDocumentStore(new Options { Server = server });
        await StoreDocument(store);
        var taskId = await Backup.UpdateConfigAsync(server, Backup.CreateBackupConfiguration(NewDataPath(suffix: "BackupFolder")), store);

        // same server-health gate as low memory / high dirty memory in BackupUtils.CheckServerHealthBeforeBackup
        server.CpuCreditsBalance.BackgroundTasksAlertRaised.Raise();
        try
        {
            Operation<StartBackupOperationResult> result = null;
            var error = await Record.ExceptionAsync(async () => result = await store.Maintenance.SendAsync(new StartBackupOperation(isFullBackup: true, taskId)));

            Assert.True(error != null,
                $"Backup Now started (operation id {result?.Id}) while the server's CPU credits were near exhaustion; the old runner rejected it with BackupDelayException.");
        }
        finally
        {
            server.CpuCreditsBalance.BackgroundTasksAlertRaised.Lower();
        }
    }

    [RavenFact(RavenTestCategory.BackupExportImport)]
    public async Task Finding10_DecisionLogMustNotRecordEveryTickForTasksThatAreNotDue()
    {
        using var server = GetNewServer(new ServerCreationOptions
        {
            CustomSettings = new Dictionary<string, string>
            {
                [RavenConfiguration.GetKey(x => x.Backup.BackupRunnerFrequency)] = "1"
            }
        });
        using var store = GetDocumentStore(new Options { Server = server });
        await StoreDocument(store);

        // full backup once a year, so the task is never due during the test
        var taskId = await Backup.UpdateConfigAndRunBackupAsync(server, Backup.CreateBackupConfiguration(NewDataPath(suffix: "BackupFolder")), store);
        var startUtc = DateTime.UtcNow;

        await Task.Delay(TimeSpan.FromSeconds(20));

        var log = Decisions(server, store.Database, taskId);
        var notDue = log.Where(d => d.Category is BackupDecisionCategory.Waiting or BackupDecisionCategory.HeldBack && d.LastTime >= startUtc).ToList();
        var distinctMessages = notDue.Select(d => d.Reason).Distinct().Count();

        // record struct (time, kind, two references) + string header + UTF-16 payload
        var bytesPerEntry = notDue.Count == 0 ? 0 : notDue.Average(d => 32 + 22 + 2 * (d.Reason?.Length ?? 0));
        var projectedKbPerTask = bytesPerEntry * BackupDecisionTracker.MaxDecisionLogSize / 1024;
        var hoursUntilLifecycleEntriesAreEvicted = BackupDecisionTracker.MaxDecisionLogSize * 60 / 3600.0;

        Assert.True(notDue.Count <= 1,
            $"{notDue.Count} 'not due' policy entries were added in 20s for a single task ({distinctMessages} distinct message(s)). " +
            $"~{bytesPerEntry:N0} bytes per entry, ~{projectedKbPerTask:N0} KB retained per task at the {BackupDecisionTracker.MaxDecisionLogSize}-entry cap; " +
            $"at the default 60s frequency the Started/Completed entries are pushed out after ~{hoursUntilLifecycleEntriesAreEvicted:N1}h.");
    }

    [RavenFact(RavenTestCategory.BackupExportImport | RavenTestCategory.Cluster)]
    public async Task Finding10_RunnerMustNotTrackBackupTasksOfDatabasesNotHostedOnThisNode()
    {
        var settings = new Dictionary<string, string>
        {
            [RavenConfiguration.GetKey(x => x.Backup.BackupRunnerFrequency)] = "1"
        };
        var (nodes, _) = await CreateRaftCluster(2, shouldRunInMemory: false, customSettings: settings);
        var hostingNode = nodes[0];
        var otherNode = nodes[1];
        var databaseName = GetDatabaseName();

        using var store = new DocumentStore { Urls = new[] { hostingNode.WebUrl }, Database = databaseName }.Initialize();
        await store.Maintenance.Server.SendAsync(new CreateDatabaseOperation(new DatabaseRecord(databaseName)
        {
            Topology = new DatabaseTopology { Members = new List<string> { hostingNode.ServerStore.NodeTag } }
        }));

        var result = await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(Backup.CreateBackupConfiguration(NewDataPath(suffix: "BackupFolder"))));
        await Cluster.WaitForRaftIndexToBeAppliedOnClusterNodesAsync(result.RaftCommandIndex, nodes);

        var info = await DisposeServerAndWaitForFinishOfDisposalAsync(otherNode);
        var revived = await ReviveNodeAsync(info, new ServerCreationOptions
        {
            RunInMemory = false,
            DeletePrevious = false,
            RegisterForDisposal = true,
            CustomSettings = new Dictionary<string, string>(settings)
        });

        await Task.Delay(TimeSpan.FromSeconds(10));

        var tracked = revived.ServerStore.ServerBackupRunner.GetDatabaseStateByTaskId(databaseName, result.TaskId);
        var log = tracked == null ? new List<BackupDecision>() : Decisions(revived, databaseName, result.TaskId);

        Assert.True(tracked == null,
            $"Node '{revived.ServerStore.NodeTag}' does not host '{databaseName}' but after restart its backup runner tracks task {result.TaskId} " +
            $"and logged {log.Count} decision(s) in ~10s:{Environment.NewLine}{Dump(log, 5)}");
    }

    [RavenFact(RavenTestCategory.BackupExportImport)]
    public async Task Finding11_IdleDatabaseMustNotBeReloadedForIncrementalBackupsWithoutChanges()
    {
        using var server = GetNewServer(new ServerCreationOptions
        {
            RunInMemory = false,
            CustomSettings = IdleSettings()
        });
        using var store = GetDocumentStore(new Options { Server = server, RunInMemory = false });
        await StoreDocument(store);

        var config = Backup.CreateBackupConfiguration(NewDataPath(suffix: "BackupFolder"), fullBackupFrequency: "0 0 1 1 *", incrementalBackupFrequency: "* * * * *");
        var taskId = await Backup.UpdateConfigAndRunBackupAsync(server, config, store);

        // no writes from now on
        var landlord = server.ServerStore.DatabasesLandlord;
        var reloads = new ConcurrentQueue<(DateTime Time, string Caller)>();
        landlord.ForTestingPurposesOnly().SkipIncreasingLastWorkTimeBasedOnDatabaseSize = true;
        landlord.ForTestingPurposesOnly().AfterDatabaseCreation = x =>
        {
            if (string.Equals(x.Database?.Name, store.Database, StringComparison.OrdinalIgnoreCase))
                reloads.Enqueue((DateTime.UtcNow, x.caller));
        };
        var startUtc = DateTime.UtcNow;

        await Task.Delay(TimeSpan.FromSeconds(150));

        var started = Decisions(server, store.Database, taskId).Count(d => d.Category == BackupDecisionCategory.Started && d.LastTime >= startUtc);
        Assert.True(reloads.IsEmpty,
            $"With no writes, database '{store.Database}' was unloaded and reloaded {reloads.Count} time(s) in 150s " +
            $"(at {string.Join(", ", reloads.Select(r => $"{r.Time:HH:mm:ss}"))}) to run {started} incremental backup(s).");
    }

    private static Dictionary<string, string> IdleSettings()
    {
        return new Dictionary<string, string>
        {
            [RavenConfiguration.GetKey(x => x.Databases.MaxIdleTime)] = "10",
            [RavenConfiguration.GetKey(x => x.Databases.FrequencyToCheckForIdle)] = "3",
            [RavenConfiguration.GetKey(x => x.Backup.BackupRunnerFrequency)] = "1"
        };
    }

    private static async Task StoreDocument(IDocumentStore store)
    {
        using (var session = store.OpenAsyncSession())
        {
            await session.StoreAsync(new User { Name = "Grisha" }, "users/1");
            await session.SaveChangesAsync();
        }
    }

    private static List<BackupDecision> Decisions(RavenServer server, string database, long taskId)
    {
        return BackupTestBase.GetDecisionLog(server.ServerStore.ServerBackupRunner, database, taskId) ?? new List<BackupDecision>();
    }

    private static string Dump(IEnumerable<BackupDecision> decisions, int take = 15)
    {
        return string.Join(Environment.NewLine, decisions.Take(take).Select(d => $"{d.LastTime:HH:mm:ss.fff} {d.Category} {d.Detail} {d.Reason}"));
    }

    private static async Task<DatabaseBackupState> WaitForStateAsync(ServerBackupRunner runner, string database, long taskId)
    {
        var sw = System.Diagnostics.Stopwatch.StartNew();
        while (sw.Elapsed < TimeSpan.FromSeconds(15))
        {
            var state = runner.GetDatabaseStateByTaskId(database, taskId);
            if (state != null)
                return state;

            await Task.Delay(100);
        }

        throw new InvalidOperationException($"precondition: runner should track backup task {taskId} of '{database}'");
    }

    private static int CountInQueue(ServerBackupRunner runner, DatabaseBackupState state)
    {
        var field = typeof(ServerBackupRunner).GetField("_backupQueue", BindingFlags.NonPublic | BindingFlags.Instance);
        var queue = (ConcurrentQueue<DatabaseBackupState>)field!.GetValue(runner);
        return queue!.Count(x => ReferenceEquals(x, state));
    }

    private static ConcurrentQueue<DatabaseBackupState> GetQueue(ServerBackupRunner runner)
    {
        var field = typeof(ServerBackupRunner).GetField("_backupQueue", BindingFlags.NonPublic | BindingFlags.Instance);
        return (ConcurrentQueue<DatabaseBackupState>)field!.GetValue(runner);
    }

    private static PeriodicBackupConfiguration GetSingleBackupConfiguration(RavenServer server, string database)
    {
        using (server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
        using (context.OpenReadTransaction())
        using (var record = server.ServerStore.Cluster.ReadRawDatabaseRecord(context, database))
            return record.PeriodicBackups.Single();
    }

    private static long GetSingleBackupTaskId(RavenServer server, string database) => GetSingleBackupConfiguration(server, database).TaskId;

    private static PeriodicBackupStatus GetClusterBackupStatus(RavenServer server, string database, long taskId)
    {
        using (server.ServerStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
        using (context.OpenReadTransaction())
            return BackupUtils.GetBackupStatusFromCluster(context, database, taskId);
    }

    private static async Task WaitForSecondOfMinuteBelow(int second)
    {
        while (DateTime.Now.Second >= second)
            await Task.Delay(250);
    }

    private static async Task WaitUntilNextMinuteBoundary(TimeSpan extra)
    {
        var now = DateTime.Now;
        var nextMinute = new DateTime(now.Year, now.Month, now.Day, now.Hour, now.Minute, 0, now.Kind).AddMinutes(1);
        await Task.Delay(nextMinute - now + extra);
    }
}
