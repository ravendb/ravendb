using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.WebSockets;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.CompareExchange;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Exceptions.Database;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Client.ServerWide.Operations.Configuration;
using Raven.Server;
using Raven.Server.Config;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Backups;
using Raven.Server.ServerWide.Backups.Policies.Database;
using Raven.Server.ServerWide.Commands.PeriodicBackup;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.PeriodicBackup;

// Repros for the review findings on RavenDB-24994 (centralized backup runner).
// Every test asserts the expected behavior, so it fails while the defect exists.
public class ServerBackupRunnerTests : ClusterTestBase
{
    public ServerBackupRunnerTests(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.BackupExportImport)]
    public async Task CpuCreditsHoldMustPauseTheRunnerUntilTheHoldEnds()
    {
        using var server = GetNewServer(new ServerCreationOptions { IgnoreHealthChecksBeforeBackup = false });
        using var store = GetDocumentStore(new Options { Server = server });
        await StoreDocument(store);
        await Backup.UpdateConfigAsync(server, Backup.CreateBackupConfiguration(NewDataPath(suffix: "BackupFolder")), store);

        var decisions = server.ServerStore.ServerBackupRunner.Decisions;
        var holdDelay = server.Configuration.Server.CpuCreditsExhaustionBackupDelay.AsTimeSpan;

        server.CpuCreditsBalance.BackgroundTasksAlertRaised.Raise();
        try
        {
            Assert.True(await WaitForValueAsync(() => GetCpuCreditsDecision(decisions) != null, true),
                "The runner never recorded the CPU credits hold.");

            await Task.Delay(server.Configuration.Backup.BackupRunnerFrequency.AsTimeSpan * 5);

            var decision = GetCpuCreditsDecision(decisions);
            Assert.True(decision.Count == 1,
                $"The runner kept checking the server policies during the CPU credits hold ({decision.Count} checks), instead of pausing until the hold ends.");

            var serverBlock = decisions.GetDecisionLogDetails(new BackupDecisionLogFilter { Take = 1 }, afterEtag: null).Summary.ServerBlock;
            Assert.NotNull(serverBlock);
            Assert.NotNull(serverBlock.Until);
            Assert.True(serverBlock.Until.Value > DateTime.UtcNow + holdDelay - TimeSpan.FromMinutes(1),
                $"The server block ends at '{serverBlock.Until}', expected about '{DateTime.UtcNow + holdDelay}'.");
        }
        finally
        {
            server.CpuCreditsBalance.BackgroundTasksAlertRaised.Lower();
        }

        static BackupDecisionLogEntry GetCpuCreditsDecision(BackupDecisionTracker decisions)
        {
            return decisions.GetDecisionLogDetails(new BackupDecisionLogFilter { Take = 100 }, afterEtag: null).Entries
                .SingleOrDefault(x => x.Source == BackupDecisionSource.Server && x.Detail == "CpuCredits");
        }
    }

    [RavenFact(RavenTestCategory.BackupExportImport)]
    public void Finding10_DecisionLogMustKeepTheNewestDecisionsWhenFull()
    {
        var log = new BackupDecisionLog(maxSize: 3);
        long lastId = 0;
        long lastEtag = 0;

        for (var i = 0; i < 5; i++)
            log.Add(BackupDecisionCategory.Waiting, $"detail {i}", reason: null, DateTime.UtcNow, () => ++lastId, () => ++lastEtag);

        log.Add(BackupDecisionCategory.Waiting, "detail 4", reason: null, DateTime.UtcNow, () => ++lastId, () => ++lastEtag);

        var decisions = log.GetDecisions();
        Assert.Equal(new[] { "detail 4", "detail 3", "detail 2" }, decisions.Select(x => x.Detail));
        Assert.Equal(2, decisions[0].Count);
        Assert.Equal(6, decisions[0].Etag);

        Assert.True(log.TryGetLatest(out var latest));
        Assert.Equal("detail 4", latest.Detail);

        Assert.Equal(new[] { "detail 4" }, log.GetDecisions(afterEtag: 5).Select(x => x.Detail));
    }

    [RavenFact(RavenTestCategory.BackupExportImport | RavenTestCategory.Cluster)]
    public async Task DecisionLogMustBeServedPerNodeAndWatchedLive()
    {
        var (nodes, leader) = await CreateRaftCluster(2, watcherCluster: true);
        using var store = GetDocumentStore(new Options { Server = leader, ReplicationFactor = 2 });
        await StoreDocument(store);
        var taskId = await Backup.UpdateConfigAsync(leader, Backup.CreateBackupConfiguration(NewDataPath(suffix: "BackupFolder")), store);

        var other = nodes.Single(x => x != leader);
        Assert.True(WaitForValue(() => BackupTestBase.GetBackupState(other.ServerStore.ServerBackupRunner, store.Database, taskId) != null, true),
            "precondition: the other node tracks the task");
        Assert.True(WaitForValue(() => Decisions(other, store.Database, taskId).Count > 0, true, timeout: 30_000, interval: 250),
            "precondition: the other node logged a decision for the task");

        using (var otherWebSocket = new ClientWebSocket())
        using (var otherCts = new CancellationTokenSource(TimeSpan.FromMinutes(1)))
        {
            await otherWebSocket.ConnectAsync(new Uri($"{leader.WebUrl.Replace("http", "ws")}/admin/periodic-backup/decision-log/watch?nodeTag={other.ServerStore.NodeTag}&database={store.Database}&taskId={taskId}&take=10"), otherCts.Token);

            using var json = await ReceiveJsonAsync(otherWebSocket, otherCts.Token);
            var root = json.RootElement;

            Assert.Equal("Snapshot", root.GetProperty("Type").GetString());
            Assert.Equal(other.ServerStore.NodeTag, root.GetProperty("NodeTag").GetString());
            Assert.Contains(store.Database, root.GetProperty("Databases").EnumerateArray().Select(x => x.GetString()));

            var entries = root.GetProperty("Entries").EnumerateArray().ToList();
            Assert.Contains(entries, x => x.GetProperty("Source").GetString() == nameof(BackupDecisionSource.Task));

            foreach (var entry in entries)
            {
                Assert.Equal(other.ServerStore.NodeTag, entry.GetProperty("NodeTag").GetString());

                if (entry.GetProperty("Source").GetString() != nameof(BackupDecisionSource.Task))
                    continue;

                Assert.Equal(store.Database, entry.GetProperty("Database").GetString());
                Assert.Equal(taskId, entry.GetProperty("TaskId").GetInt64());
            }
        }

        var runner = leader.ServerStore.ServerBackupRunner;
        var database = await Databases.GetDocumentDatabaseInstanceFor(leader, store);
        var state = await WaitForStateAsync(runner, store.Database, taskId);
        Assert.False(WaitForValue(() => state.Running.IsRaised(), false, timeout: 60_000, interval: 100), "precondition: no backup is running");

        using var webSocket = new ClientWebSocket();
        using var cts = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        await webSocket.ConnectAsync(new Uri($"{leader.WebUrl.Replace("http", "ws")}/admin/periodic-backup/decision-log/watch?database={store.Database}&take=10"), cts.Token);

        int snapshotTotal;
        using (var snapshot = await ReceiveJsonAsync(webSocket, cts.Token))
        {
            Assert.Equal("Snapshot", snapshot.RootElement.GetProperty("Type").GetString());
            Assert.Equal(leader.ServerStore.NodeTag, snapshot.RootElement.GetProperty("NodeTag").GetString());
            Assert.True(snapshot.RootElement.TryGetProperty("Summary", out _));
            snapshotTotal = snapshot.RootElement.GetProperty("TotalResults").GetInt32();
        }

        var hold = new TaskCompletionSource<object>(TaskCreationOptions.RunContinuationsAsynchronously);
        runner.ForTestingPurposesOnly().DatabaseTestingStuffInternals[store.Database] = new ServerBackupRunner.TestingStuffInternal { OnBackupTaskRunHoldBackupExecution = hold };

        try
        {
            var operationId = runner.StartBackupNowTask(database, taskId, isFullBackup: true, database.Operations.GetNextOperationId());
            Assert.True(WaitForValue(() => state.RunningTask != null, true, timeout: 15_000, interval: 50), "precondition: the backup is running");

            await database.Operations.KillOperationAsync(operationId, CancellationToken.None);
        }
        finally
        {
            hold.TrySetResult(null);
        }

        var sawStarted = false;
        string cancelDetail = null;
        var totalResults = snapshotTotal;
        while (cancelDetail == null)
        {
            using var update = await ReceiveJsonAsync(webSocket, cts.Token);
            Assert.Equal("Update", update.RootElement.GetProperty("Type").GetString());
            Assert.Equal(JsonValueKind.Object, update.RootElement.GetProperty("Summary").ValueKind);
            Assert.Equal(JsonValueKind.Null, update.RootElement.GetProperty("Databases").ValueKind);
            Assert.Equal(JsonValueKind.Null, update.RootElement.GetProperty("Tasks").ValueKind);
            totalResults = update.RootElement.GetProperty("TotalResults").GetInt32();

            foreach (var entry in update.RootElement.GetProperty("Entries").EnumerateArray())
            {
                var entryTaskId = entry.GetProperty("TaskId");
                if (entryTaskId.ValueKind != JsonValueKind.Number || entryTaskId.GetInt64() != taskId)
                    continue;

                var category = entry.GetProperty("Category").GetString();
                if (category == nameof(BackupDecisionCategory.Started))
                    sawStarted = true;
                else if (category == nameof(BackupDecisionCategory.Cancelled))
                    cancelDetail = entry.GetProperty("Detail").GetString();
            }
        }

        Assert.True(sawStarted, "The watch should send the Started decision of the backup");
        Assert.Equal(BackupDecisionTracker.CancelReasonUser, cancelDetail);
        Assert.True(totalResults >= snapshotTotal + 2,
            $"The update should count all matching entries, including the new Started and Cancelled ones (snapshot: {snapshotTotal}, last update: {totalResults})");
    }

    [RavenTheory(RavenTestCategory.BackupExportImport | RavenTestCategory.Cluster)]
    [InlineData(false)]
    [InlineData(true)]
    public async Task IdleDatabasesMustSaveSkippedIncrementalBackupsInBatchesAndBackUpCompareExchangeChanges(bool mixedVersionCluster)
    {
        var settings = new Dictionary<string, string>(IdleSettings())
        {
            [RavenConfiguration.GetKey(x => x.Backup.MaxStatusUpdatesPerBatch)] = "2"
        };

        if (mixedVersionCluster)
            settings[RavenConfiguration.GetKey(x => x.Cluster.MaximalAllowedClusterVersion)] = (ClusterCommandsVersionManager.ClusterCommandsVersions[nameof(UpdatePeriodicBackupStatusBatchCommand)] - 1).ToString();

        var (nodes, server) = await CreateRaftCluster(2, shouldRunInMemory: false, customSettings: settings);
        var other = nodes.Single(x => x != server);

        var batchSupported = mixedVersionCluster == false;
        Assert.True(WaitForValue(() => server.ServerStore.Engine.CommandsVersionManager.CanPutCommand(nameof(UpdatePeriodicBackupStatusBatchCommand)), batchSupported) == batchSupported,
            $"precondition: the batch command should {(mixedVersionCluster ? "not " : string.Empty)}be supported by the cluster");

        var stores = new List<(DocumentStore Store, long TaskId)>();

        try
        {
            for (var i = 0; i < 3; i++)
            {
                var store = GetDocumentStore(new Options
                {
                    Server = server,
                    RunInMemory = false,
                    ReplicationFactor = 1,
                    ModifyDatabaseRecord = record => record.Topology = new DatabaseTopology { Members = new List<string> { server.ServerStore.NodeTag } }
                });
                await StoreDocument(store);

                var config = Backup.CreateBackupConfiguration(NewDataPath(suffix: $"BackupFolder{i}"), fullBackupFrequency: "0 0 1 1 *", incrementalBackupFrequency: "* * * * *");
                stores.Add((store, await Backup.UpdateConfigAndRunBackupAsync(server, config, store)));
            }

            var lastIncrementalBackups = stores.ToDictionary(x => x.Store.Database, x => GetClusterBackupStatus(server, x.Store.Database, x.TaskId).LastIncrementalBackup);

            server.ServerStore.DatabasesLandlord.ForTestingPurposesOnly().SkipIncreasingLastWorkTimeBasedOnDatabaseSize = true;
            Assert.True(WaitForValue(() => stores.All(x => server.ServerStore.IdleDatabases.ContainsKey(x.Store.Database)), true, timeout: 90_000, interval: 500),
                "precondition: all databases should become idle");

            foreach (var (store, taskId) in stores)
            {
                Assert.True(await WaitForValueAsync(() => Task.FromResult(GetClusterBackupStatus(server, store.Database, taskId).LastIncrementalBackup != lastIncrementalBackups[store.Database]), true,
                        timeout: 150_000, interval: 1000),
                    $"The skipped incremental backup of '{store.Database}' wasn't saved.{Environment.NewLine}{Dump(Decisions(server, store.Database, taskId))}");

                var status = GetClusterBackupStatus(server, store.Database, taskId);
                Assert.Null(status.Error);
                Assert.Equal(server.ServerStore.NodeTag, status.NodeTag);
                Assert.True(server.ServerStore.IdleDatabases.ContainsKey(store.Database), $"'{store.Database}' should still be idle");

                Assert.True(await WaitForValueAsync(() => Task.FromResult(GetClusterBackupStatus(other, store.Database, taskId).LastIncrementalBackup == status.LastIncrementalBackup), true),
                    $"The skipped incremental backup status of '{store.Database}' should reach node '{other.ServerStore.NodeTag}'");
            }

            var (changedStore, changedTaskId) = stores[0];

            await WaitForSecondOfMinuteBelow(40);
            var putUtc = DateTime.UtcNow;
            var put = await changedStore.Operations.SendAsync(new PutCompareExchangeValueOperation<User>("users/lock", new User { Name = "Grisha" }, 0));
            Assert.True(put.Successful, "precondition: compare exchange value should be saved");

            BackupDecision completed = null;
            Assert.True(await WaitForValueAsync(() =>
                {
                    completed = Decisions(server, changedStore.Database, changedTaskId).FirstOrDefault(d => d.Category == BackupDecisionCategory.Completed && d.LastTime >= putUtc);
                    return Task.FromResult(completed != null);
                }, true, timeout: 120_000, interval: 1000),
                $"The incremental backup after a compare exchange change was skipped.{Environment.NewLine}{Dump(Decisions(server, changedStore.Database, changedTaskId))}");

            var skipped = Decisions(server, changedStore.Database, changedTaskId).Where(d => d.Detail == "NoChanges" && d.LastTime >= putUtc && d.LastTime <= completed.LastTime).ToList();
            Assert.True(skipped.Count == 0, $"The incremental backup after a compare exchange change was skipped.{Environment.NewLine}{Dump(skipped)}");
        }
        finally
        {
            foreach (var (store, _) in stores)
                store.Dispose();
        }
    }

    [RavenFact(RavenTestCategory.BackupExportImport | RavenTestCategory.Cluster)]
    public async Task ScheduledBackupMustRunWhenTheNodeHasNoQuorum()
    {
        var settings = new Dictionary<string, string>
        {
            [RavenConfiguration.GetKey(x => x.Cluster.OperationTimeout)] = "1"
        };

        var (nodes, server) = await CreateRaftCluster(2, shouldRunInMemory: false, customSettings: settings);
        var other = nodes.Single(x => x != server);

        using var store = GetDocumentStore(new Options
        {
            Server = server,
            RunInMemory = false,
            ReplicationFactor = 1,
            DeleteDatabaseOnDispose = false,
            ModifyDatabaseRecord = record => record.Topology = new DatabaseTopology { Members = new List<string> { server.ServerStore.NodeTag } }
        });
        await StoreDocument(store);

        var config = Backup.CreateBackupConfiguration(NewDataPath(suffix: "BackupFolder"), fullBackupFrequency: "* * * * *");
        var taskId = await Backup.UpdateConfigAndRunBackupAsync(server, config, store);
        var lastFullBackup = server.ServerStore.DatabaseInfoCache.BackupStatusStorage.GetBackupStatus(store.Database, taskId)?.LastFullBackup;

        await WaitForSecondOfMinuteBelow(30);
        await DisposeServerAndWaitForFinishOfDisposalAsync(other);
        await WaitAndAssertForValueAsync(() => server.ServerStore.Engine.CurrentState, RachisState.Candidate);
        var noQuorumUtc = DateTime.UtcNow;

        Assert.True(await WaitForValueAsync(() => Task.FromResult(Decisions(server, store.Database, taskId)
                    .Any(d => d.Category == BackupDecisionCategory.Completed && d.LastTime >= noQuorumUtc)), true, timeout: 180_000, interval: 1000),
            $"The scheduled backup didn't run while the node had no quorum.{Environment.NewLine}{Dump(Decisions(server, store.Database, taskId))}");

        var localStatus = server.ServerStore.DatabaseInfoCache.BackupStatusStorage.GetBackupStatus(store.Database, taskId);
        Assert.True(localStatus != null && localStatus.LastFullBackup > lastFullBackup,
            $"The status of the backup that ran without quorum wasn't saved locally. Before: '{lastFullBackup}', after: '{localStatus?.LastFullBackup}'.");
    }

    [RavenFact(RavenTestCategory.BackupExportImport)]
    public async Task IdleDatabaseMustUnloadWhileACpuCreditsHoldHoldsItsBackup()
    {
        using var server = GetNewServer(new ServerCreationOptions { RunInMemory = false, IgnoreHealthChecksBeforeBackup = false, CustomSettings = IdleSettings() });
        using var store = GetDocumentStore(new Options { Server = server, RunInMemory = false });
        await StoreDocument(store);

        server.CpuCreditsBalance.BackgroundTasksAlertRaised.Raise();

        try
        {
            var taskId = await Backup.UpdateConfigAsync(server, Backup.CreateBackupConfiguration(NewDataPath(suffix: "BackupFolder"), fullBackupFrequency: "* * * * *"), store);
            var state = await WaitForStateAsync(server.ServerStore.ServerBackupRunner, store.Database, taskId);
            Assert.True(WaitForValue(() => state.NextBackup != null, true), "precondition: the first backup is scheduled");

            server.ServerStore.DatabasesLandlord.ForTestingPurposesOnly().SkipIncreasingLastWorkTimeBasedOnDatabaseSize = true;

            Assert.True(WaitForValue(() => server.ServerStore.IdleDatabases.ContainsKey(store.Database), true, timeout: 150_000, interval: 500),
                $"The idle database stayed loaded although a CPU credits hold holds its backup. Next backup: '{state.NextBackup?.DateTime}'.{Environment.NewLine}{Dump(Decisions(server, store.Database, taskId))}");

            Assert.DoesNotContain(Decisions(server, store.Database, taskId), d => d.Category == BackupDecisionCategory.Started);
        }
        finally
        {
            server.CpuCreditsBalance.BackgroundTasksAlertRaised.Lower();
        }
    }

    [RavenFact(RavenTestCategory.BackupExportImport)]
    public async Task ScheduledBackupsMustWaitForTheServerStartupGracePeriod()
    {
        var gracePeriod = TimeSpan.FromSeconds(90);
        using var server = GetNewServer(new ServerCreationOptions
        {
            CustomSettings = new Dictionary<string, string>
            {
                [RavenConfiguration.GetKey(x => x.Backup.ServerStartupGracePeriod)] = ((int)gracePeriod.TotalSeconds).ToString()
            }
        });
        using var store = GetDocumentStore(new Options { Server = server });
        await StoreDocument(store);

        var taskId = await Backup.UpdateConfigAsync(server, Backup.CreateBackupConfiguration(NewDataPath(suffix: "BackupFolder"), fullBackupFrequency: "* * * * *"), store);
        var graceEndUtc = server.Statistics.StartUpTime + gracePeriod;

        BackupDecision started = null;
        Assert.True(await WaitForValueAsync(() =>
            {
                started = Decisions(server, store.Database, taskId).LastOrDefault(d => d.Category == BackupDecisionCategory.Started);
                return Task.FromResult(started != null);
            }, true, timeout: 180_000, interval: 1000),
            $"The scheduled backup didn't start after the server startup grace period.{Environment.NewLine}{Dump(Decisions(server, store.Database, taskId))}");

        Assert.True(started.FirstTime >= graceEndUtc,
            $"The scheduled backup started at '{started.FirstTime:HH:mm:ss.fff}', before the server startup grace period ended at '{graceEndUtc:HH:mm:ss.fff}'.");
    }

    private static async Task<JsonDocument> ReceiveJsonAsync(ClientWebSocket socket, CancellationToken token)
    {
        var buffer = new byte[64 * 1024];
        using var ms = new MemoryStream();

        while (true)
        {
            ms.SetLength(0);

            while (true)
            {
                var result = await socket.ReceiveAsync(new ArraySegment<byte>(buffer), token);
                ms.Write(buffer, 0, result.Count);

                if (result.EndOfMessage)
                    break;
            }

            if (ms.ToArray().SequenceEqual(WebSocketHelper.Heartbeat))
                continue;

            ms.Position = 0;
            return await JsonDocument.ParseAsync(ms, cancellationToken: token);
        }
    }

    private static Dictionary<string, string> IdleSettings(int keepIdleDatabaseLoadedBeforeBackupInMin = 5)
    {
        return new Dictionary<string, string>
        {
            [RavenConfiguration.GetKey(x => x.Databases.MaxIdleTime)] = "10",
            [RavenConfiguration.GetKey(x => x.Databases.FrequencyToCheckForIdle)] = "3",
            [RavenConfiguration.GetKey(x => x.Backup.KeepIdleDatabaseLoadedBeforeBackup)] = keepIdleDatabaseLoadedBeforeBackupInMin.ToString()
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
        return string.Join(Environment.NewLine, decisions.Take(take).Select(d => $"{d.LastTime:HH:mm:ss.fff} x{d.Count} {d.Category} {d.Detail} {d.Reason}"));
    }

    private async Task<DatabaseBackupState> WaitForStateAsync(ServerBackupRunner runner, string database, long taskId)
    {
        DatabaseBackupState state = null;

        Assert.True(await WaitForValueAsync(() => Task.FromResult((state = BackupTestBase.GetBackupState(runner, database, taskId)) != null), true, timeout: 15_000, interval: 100),
            $"precondition: runner should track backup task {taskId} of '{database}'");

        return state;
    }

    private static int CountInQueue(ServerBackupRunner runner, DatabaseBackupState state)
    {
        return runner.ForTestingPurposesOnly().CountInQueue(state);
    }

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

}
