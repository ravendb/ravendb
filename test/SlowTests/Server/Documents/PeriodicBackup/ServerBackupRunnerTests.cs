using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
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
