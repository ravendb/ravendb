using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using System.Web;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.ServerWide.Operations;
using Raven.Client.ServerWide.Operations.Certificates;
using Raven.Client.ServerWide.Operations.Configuration;
using Raven.Client.Util;
using Raven.Server;
using Raven.Server.Config;
using Raven.Server.Documents;
using Raven.Server.Documents.PeriodicBackup;
using Raven.Server.Documents.PeriodicBackup.BackupHistory;
using Raven.Server.Documents.TransactionMerger.Commands;
using Raven.Server.Json;
using Raven.Server.Utils;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Backups;
using Raven.Server.ServerWide.Context;
using SlowTests.Core.Utils.Entities;
using Sparrow.Json;
using Sparrow.Server.Json.Sync;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Documents.PeriodicBackup;

public class BackupHistoryTests : ClusterTestBase
{
    public BackupHistoryTests(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.BackupExportImport)]
    public async Task BackupHistory_ShouldReturnCompleteAndAccurateEndpointResponses()
    {
        const string taskName = "RavenDB-19358 Backup history assert endpoints responses";
        var backupPath = NewDataPath(suffix: "BackupFolder");
        using var store = GetDocumentStore(new Options { RunInMemory = true });

        using (var session = store.OpenAsyncSession())
        {
            await session.StoreAsync(new User { Name = "RavenDB-19358" }, "user/1");
            await session.SaveChangesAsync();
        }

        var config = Backup.CreateBackupConfiguration(backupPath, fullBackupFrequency: "0 0 1 1 1", name: taskName);
        var result = await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config));
        var taskId = result.TaskId;
        await Cluster.WaitForRaftIndexToBeAppliedOnClusterNodesAsync(taskId, nodes: [Server]);

        var fullBackupStatus = await Backup.RunBackupAndReturnStatusAsync(Server, taskId, store, isFullBackup: true);
        var incrementalBackupStatus = await Backup.RunBackupAndReturnStatusAsync(Server, taskId, store, isFullBackup: false);

        BackupHistory backupHistory = null;
        BackupResult fullBackupResult = null;
        BackupResult incrementalBackupResult = null;
        using (Server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
        using (context.OpenReadTransaction())
        {
            var parameters = new BackupHistoryRequestParameters{ DatabaseName = store.Database };

            WaitForValue(() =>
            {
                backupHistory = GetBackupHistoryFromEndpoint(context, store, parameters);
                fullBackupResult = GetBackupResultFromEndpoint(context, store, store.Database, taskId, backupHistory.Groups[0].FullBackup.CreatedAt);
                incrementalBackupResult = GetBackupResultFromEndpoint(context, store, store.Database, taskId, backupHistory.Groups[0].IncrementalBackups[0].CreatedAt);

                return backupHistory.Groups.Count == 1 && fullBackupResult != null && incrementalBackupResult != null;
            },
                expectedVal: true,
                timeout: (int)TimeSpan.FromSeconds(15).TotalMilliseconds,
                interval: (int)TimeSpan.FromSeconds(1).TotalMilliseconds);
        }

        // BackupHistory fields assertions
        Assert.Equal(store.Database, backupHistory.DatabaseName);
        var backupGroup = backupHistory.Groups.SingleOrDefault();
        Assert.True(backupGroup != null, userMessage: $"Single group expected, but got `{backupHistory.Groups.Count}`{Environment.NewLine}{backupHistory.ToString(Server.ServerStore.ContextPool)}");

        // Backup results should be stored
        Assert.NotNull(fullBackupResult);
        Assert.NotNull(incrementalBackupResult);

        // Group fields assertions
        Assert.Equal(taskId, backupGroup.TaskId);
        Assert.Equal(taskName, backupGroup.TaskName);
        Assert.Equal(backupGroup.IncrementalBackups.Count, backupGroup.IncrementalBackupsCount);

        // FullBackup fields assertions
        var fullBackupHistoryEntry = backupGroup.FullBackup;
        Assert.Equal(BackupKind.Full, fullBackupHistoryEntry.BackupKind);
        Assert.True(fullBackupStatus.LastFullBackup.HasValue);
        Assert.Equal(fullBackupStatus.LastFullBackup, fullBackupHistoryEntry.CreatedAt);
        Assert.Equal(fullBackupStatus.BackupType, fullBackupHistoryEntry.BackupType);
        Assert.Equal(fullBackupStatus.DurationInMs, fullBackupHistoryEntry.DurationInMs);
        Assert.Equal(fullBackupStatus.Error?.Exception, fullBackupHistoryEntry.Error);
        Assert.Equal(fullBackupStatus.NodeTag, fullBackupHistoryEntry.NodeTag);
        Assert.Equal(fullBackupStatus.LastFullBackup, fullBackupHistoryEntry.LastFullBackup);

        // IncrementalBackup fields assertions
        var incrementalBackupHistoryEntry = backupGroup.IncrementalBackups.SingleOrDefault();
        Assert.True(incrementalBackupHistoryEntry != null, $"Single incremental expected, but got `{backupGroup.IncrementalBackups.Count}`{Environment.NewLine}{backupHistory.ToString(Server.ServerStore.ContextPool)}");
        Assert.Equal(BackupKind.Incremental, incrementalBackupHistoryEntry.BackupKind);
        Assert.True(incrementalBackupStatus.LastIncrementalBackup.HasValue);
        Assert.Equal(incrementalBackupStatus.LastIncrementalBackup, incrementalBackupHistoryEntry.CreatedAt);
        Assert.Equal(incrementalBackupStatus.DurationInMs, incrementalBackupHistoryEntry.DurationInMs);
        Assert.Equal(incrementalBackupStatus.Error?.Exception, incrementalBackupHistoryEntry.Error);
        Assert.Equal(incrementalBackupStatus.NodeTag, incrementalBackupHistoryEntry.NodeTag);
        Assert.Equal(incrementalBackupStatus.LastFullBackup, incrementalBackupHistoryEntry.LastFullBackup);
        Assert.Equal(fullBackupHistoryEntry.CreatedAt, incrementalBackupHistoryEntry.LastFullBackup);

        // FullBackupResult fields assertions
        Assert.True(fullBackupResult.Documents.Processed);
        Assert.Equal(1, fullBackupResult.Documents.ReadCount);

        var expectedBackupDirectory =
            $"{fullBackupHistoryEntry.CreatedAt.ToLocalTime().ToString(BackupTask.DateTimeFormat, CultureInfo.InvariantCulture)}.ravendb" +
            $"-{backupHistory.DatabaseName}" +
            $"-{fullBackupHistoryEntry.NodeTag}" +
            $"-{fullBackupHistoryEntry.BackupType.ToString().ToLower()}";

        Assert.Equal(expectedBackupDirectory, fullBackupResult.LocalBackup.BackupDirectory);

        // IncrementalBackupResult fields assertions
        Assert.True(incrementalBackupResult.Documents.Processed);
        Assert.Equal(0, incrementalBackupResult.Documents.ReadCount);
        Assert.Null(incrementalBackupResult.LocalBackup.BackupDirectory);
    }

    [RavenFact(RavenTestCategory.BackupExportImport)]
    public async Task BackupHistory_ShouldStoreFaultedBackups()
    {
        const string taskName = "RavenDB-19358 Backup History should store faulted backups";
        var backupPath = NewDataPath(suffix: "BackupFolder");
        using var store = GetDocumentStore();

        using (var session = store.OpenAsyncSession())
        {
            await session.StoreAsync(new User { Name = "RavenDB-19358" }, "user/1");
            await session.SaveChangesAsync();
        }

        var config = Backup.CreateBackupConfiguration(backupPath, fullBackupFrequency: "0 0 1 1 1", name: taskName);
        var result = await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config));
        var taskId = result.TaskId;
        await Cluster.WaitForRaftIndexToBeAppliedOnClusterNodesAsync(taskId, nodes: [Server]);

        var documentDatabase = await Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database).ConfigureAwait(false);
        var testingStuff = new ServerBackupRunner.TestingStuffInternal { SimulateFailedBackup = true };
        documentDatabase.ServerStore.ServerBackupRunner.ForTestingPurposesOnly().DatabaseTestingStuffInternals[documentDatabase.Name] = testingStuff;

        await Backup.RunBackupAsync(Server, taskId, store, isFullBackup: true, OperationStatus.Faulted);

        BackupHistory backupHistory = null;
        BackupResult backupResult;
        using (Server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
        using (context.OpenReadTransaction())
        {
            var parameters = new BackupHistoryRequestParameters { DatabaseName = store.Database };

            WaitForValue(() =>
            {
                backupHistory = GetBackupHistoryFromEndpoint(context, store, parameters);
                return backupHistory.Groups.Count;
            },
                expectedVal: 1,
                timeout: (int)TimeSpan.FromSeconds(15).TotalMilliseconds,
                interval: (int)TimeSpan.FromSeconds(1).TotalMilliseconds);

            Assert.NotNull(backupHistory);

            backupResult = GetBackupResultFromEndpoint(context, store, store.Database, taskId, backupHistory.Groups[0].FullBackup.CreatedAt);
        }

        // Group fields assertions
        var singleGroup = backupHistory.Groups.SingleOrDefault();
        Assert.True(singleGroup != null, userMessage: $"Single group expected, but got `{backupHistory.Groups.Count}`{Environment.NewLine}{backupHistory.ToString(Server.ServerStore.ContextPool)}");
        Assert.Equal(taskId, singleGroup.TaskId);
        Assert.Equal(taskName, singleGroup.TaskName);

        // Backup fields assertions
        var fullBackup = singleGroup.FullBackup;
        Assert.Equal(BackupKind.Full, fullBackup.BackupKind);
        Assert.Equal(BackupType.Backup, fullBackup.BackupType);

        // Error field assertion
        var expectedError = new Exception(nameof(ServerBackupRunner.TestingStuffInternal.SimulateFailedBackup)).ToString();
        Assert.True(fullBackup.Error.Contains(expectedError));

        // FullBackupResult existence assertion
        Assert.NotNull(backupResult);
        Assert.Equal(0, backupResult.Documents.ReadCount);
        Assert.True(backupResult.Elapsed > TimeSpan.Zero);
    }

    [RavenTheory(RavenTestCategory.BackupExportImport)]
    [RavenData(Data = [false])]
    [RavenData(Data = [true])]
    public async Task BackupHistory_ShouldBeCorrect_WithMultipleDatabases_AndMultipleBackupTasks_WithoutFiltering(Options options, bool includeIncrementals)
    {
        var firstBackupPlan = new[]
        {
            BackupKind.Full, BackupKind.Incremental,
            BackupKind.Full, BackupKind.Incremental, BackupKind.Incremental
        };

        var secondBackupPlan = new[]
        {
            BackupKind.Full, BackupKind.Incremental, BackupKind.Incremental, BackupKind.Incremental,
            BackupKind.Full, BackupKind.Incremental, BackupKind.Incremental, BackupKind.Incremental, BackupKind.Incremental,
        };

        var backupPath = NewDataPath(suffix: "BackupFolder");

        foreach (var _ in Enumerable.Range(0, 2))
        {
            using var store = GetDocumentStore(new Options { RunInMemory = true });
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new User { Name = "RavenDB-19358" }, "user/1");
                await session.SaveChangesAsync();
            }

            var firstBackupConfiguration = Backup.CreateBackupConfiguration(backupPath, fullBackupFrequency: "0 0 1 1 1", mentorNode: Server.ServerStore.NodeTag, name: $"BackupTask_{store.Database}_1");
            var firstResult = store.Maintenance.Send(new UpdatePeriodicBackupOperation(firstBackupConfiguration));
            var firstTaskId = firstResult.TaskId;
            await Cluster.WaitForRaftIndexToBeAppliedOnClusterNodesAsync(firstTaskId, nodes: [Server]);

            var expectedBackupHistory  = await RunBackupsAccordingPlan(firstBackupPlan, Server, firstTaskId, store);

            var secondBackupConfiguration = Backup.CreateBackupConfiguration(backupPath, fullBackupFrequency: "0 0 1 1 1", mentorNode: Server.ServerStore.NodeTag, name: $"BackupTask_{store.Database}_2");
            var secondResult = store.Maintenance.Send(new UpdatePeriodicBackupOperation(secondBackupConfiguration));
            var secondTaskId = secondResult.TaskId;
            await Cluster.WaitForRaftIndexToBeAppliedOnClusterNodesAsync(secondTaskId, nodes: [Server]);

            await RunBackupsAccordingPlan(secondBackupPlan, Server, secondTaskId, store, expectedBackupHistory);

            var parameters = new BackupHistoryRequestParameters{ DatabaseName = store.Database, IncludeIncrementals = includeIncrementals};
            CollectAndAssertBackupHistoryStructure(expectedBackupHistory, Server, store, parameters);
        }
    }

    [RavenTheory(RavenTestCategory.BackupExportImport)]
    [RavenData(Data = [false])]
    [RavenData(Data = [true])]
    public async Task BackupHistory_ShouldBeCorrect_WithMultipleDatabases_AndMultipleBackupTasks_FilteredByTaskId(Options options, bool includeIncrementals)
    {
        var firstBackupPlan = new[]
        {
            BackupKind.Full, BackupKind.Incremental,
            BackupKind.Full, BackupKind.Incremental, BackupKind.Incremental
        };

        var secondBackupPlan = new[]
        {
            BackupKind.Full, BackupKind.Incremental, BackupKind.Incremental, BackupKind.Incremental,
            BackupKind.Full, BackupKind.Incremental, BackupKind.Incremental, BackupKind.Incremental, BackupKind.Incremental,
        };

        var backupPath = NewDataPath(suffix: "BackupFolder");

        foreach (var _ in Enumerable.Range(0, 2))
        {
            using var store = GetDocumentStore(new Options { RunInMemory = true });
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new User { Name = "RavenDB-19358" }, "user/1");
                await session.SaveChangesAsync();
            }

            var firstBackupConfiguration = Backup.CreateBackupConfiguration(backupPath, fullBackupFrequency: "0 0 1 1 1", mentorNode: Server.ServerStore.NodeTag, name: $"BackupTask_{store.Database}_1");
            var firstResult = store.Maintenance.Send(new UpdatePeriodicBackupOperation(firstBackupConfiguration));
            var firstTaskId = firstResult.TaskId;
            await Cluster.WaitForRaftIndexToBeAppliedOnClusterNodesAsync(firstTaskId, nodes: [Server]);

            var expectedBackupHistory  = await RunBackupsAccordingPlan(firstBackupPlan, Server, firstTaskId, store);

            var secondBackupConfiguration = Backup.CreateBackupConfiguration(backupPath, fullBackupFrequency: "0 0 1 1 1", mentorNode: Server.ServerStore.NodeTag, name: $"BackupTask_{store.Database}_2");
            var secondResult = store.Maintenance.Send(new UpdatePeriodicBackupOperation(secondBackupConfiguration));
            var secondTaskId = secondResult.TaskId;
            await Cluster.WaitForRaftIndexToBeAppliedOnClusterNodesAsync(secondTaskId, nodes: [Server]);

            await RunBackupsAccordingPlan(secondBackupPlan, Server, secondTaskId, store, expectedBackupHistory);

            expectedBackupHistory.Groups.RemoveAll(x => x.TaskId != secondTaskId);
            var parameters = new BackupHistoryRequestParameters{ DatabaseName = store.Database, TaskId = secondTaskId, IncludeIncrementals = includeIncrementals};
            CollectAndAssertBackupHistoryStructure(expectedBackupHistory, Server, store, parameters);
        }
    }

    [RavenTheory(RavenTestCategory.BackupExportImport)]
    [RavenData(Data = [false])]
    [RavenData(Data = [true])]
    public async Task BackupHistory_ShouldBeCorrect_WithMultipleDatabases_AndMultipleBackupTasks_FilteredByFullBackupId(Options options, bool includeIncrementals)
    {
        var firstBackupPlan = new[]
        {
            BackupKind.Full, BackupKind.Incremental,
            BackupKind.Full, BackupKind.Incremental, BackupKind.Incremental
        };

        var secondBackupPlan = new[]
        {
            BackupKind.Full, BackupKind.Incremental, BackupKind.Incremental, BackupKind.Incremental,
            BackupKind.Full, BackupKind.Incremental, BackupKind.Incremental, BackupKind.Incremental, BackupKind.Incremental,
            BackupKind.Full, BackupKind.Incremental, BackupKind.Incremental, BackupKind.Incremental, BackupKind.Incremental, BackupKind.Incremental
        };

        var backupPath = NewDataPath(suffix: "BackupFolder");

        foreach (var _ in Enumerable.Range(0, 2))
        {
            using var store = GetDocumentStore(new Options { RunInMemory = true });
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new User { Name = "RavenDB-19358" }, "user/1");
                await session.SaveChangesAsync();
            }

            var firstBackupConfiguration = Backup.CreateBackupConfiguration(backupPath, fullBackupFrequency: "0 0 1 1 1", mentorNode: Server.ServerStore.NodeTag, name: $"BackupTask_{store.Database}_1");
            var firstResult = store.Maintenance.Send(new UpdatePeriodicBackupOperation(firstBackupConfiguration));
            var firstTaskId = firstResult.TaskId;
            await Cluster.WaitForRaftIndexToBeAppliedOnClusterNodesAsync(firstTaskId, nodes: [Server]);

            var expectedBackupHistory  = await RunBackupsAccordingPlan(firstBackupPlan, Server, firstTaskId, store);

            var secondBackupConfiguration = Backup.CreateBackupConfiguration(backupPath, fullBackupFrequency: "0 0 1 1 1", mentorNode: Server.ServerStore.NodeTag, name: $"BackupTask_{store.Database}_2");
            var secondResult = store.Maintenance.Send(new UpdatePeriodicBackupOperation(secondBackupConfiguration));
            var secondTaskId = secondResult.TaskId;
            await Cluster.WaitForRaftIndexToBeAppliedOnClusterNodesAsync(secondTaskId, nodes: [Server]);

            await RunBackupsAccordingPlan(secondBackupPlan, Server, secondTaskId, store, expectedBackupHistory);

            var parameters = new BackupHistoryRequestParameters{ DatabaseName = store.Database, TaskId = secondTaskId, IncludeIncrementals = false};
            BackupHistory fullBackupHistory;

            using (Server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
                fullBackupHistory = GetBackupHistoryFromEndpoint(context, store, parameters);

            var fullBackupId = fullBackupHistory.Groups[1].FullBackup.CreatedAt.Ticks;

            expectedBackupHistory.Groups.RemoveAll(x => x.TaskId != secondTaskId);
            expectedBackupHistory.Groups.RemoveAt(2);
            expectedBackupHistory.Groups.RemoveAt(0);
            parameters = new BackupHistoryRequestParameters{ DatabaseName = store.Database, TaskId = secondTaskId, IncludeIncrementals = includeIncrementals, FullBackupId = fullBackupId};
            CollectAndAssertBackupHistoryStructure(expectedBackupHistory, Server, store, parameters);
        }
    }

    // #4: a failed backup overwrote the successful history row of the same time
    [RavenFact(RavenTestCategory.BackupExportImport)]
    public async Task BackupHistory_FaultedBackupShouldNotOverwriteThePreviousSuccessfulEntry()
    {
        const string taskName = "RavenDB-19358 Faulted backup should not overwrite the previous successful entry";
        var backupPath = NewDataPath(suffix: "BackupFolder");
        using var store = GetDocumentStore(new Options { RunInMemory = true });

        using (var session = store.OpenAsyncSession())
        {
            await session.StoreAsync(new User { Name = "RavenDB-19358" }, "user/1");
            await session.SaveChangesAsync();
        }

        var config = Backup.CreateBackupConfiguration(backupPath, fullBackupFrequency: "0 0 1 1 1", name: taskName);
        var result = await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config));
        var taskId = result.TaskId;
        await Cluster.WaitForRaftIndexToBeAppliedOnClusterNodesAsync(taskId, nodes: [Server]);

        var successfulStatus = await Backup.RunBackupAndReturnStatusAsync(Server, taskId, store, isFullBackup: true);
        Assert.True(successfulStatus.LastFullBackup.HasValue);
        var successfulBackupCreatedAt = successfulStatus.LastFullBackup.Value;

        var documentDatabase = await Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
        documentDatabase.ServerStore.ServerBackupRunner.ForTestingPurposesOnly().DatabaseTestingStuffInternals[documentDatabase.Name] =
            new ServerBackupRunner.TestingStuffInternal { SimulateFailedBackup = true };

        await Backup.RunBackupAsync(Server, taskId, store, isFullBackup: true, OperationStatus.Faulted);

        using (Server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
        {
            var backupHistory = GetBackupHistoryFromEndpoint(context, store, new BackupHistoryRequestParameters { DatabaseName = store.Database });

            var successfulGroup = backupHistory.Groups.SingleOrDefault(group => group.FullBackup?.CreatedAt == successfulBackupCreatedAt);
            Assert.True(successfulGroup != null, $"The successful full backup created at `{successfulBackupCreatedAt:O}` is gone{Environment.NewLine}{backupHistory.ToString(Server.ServerStore.ContextPool)}");
            Assert.Null(successfulGroup.FullBackup.Error);
            Assert.Contains(backupHistory.Groups, group => group.FullBackup?.Error != null);

            var successfulResult = GetBackupResultFromEndpoint(context, store, store.Database, taskId, successfulBackupCreatedAt);
            Assert.NotNull(successfulResult);
            Assert.Equal(1, successfulResult.Documents.ReadCount);
        }
    }

    // #19: a request for one group without its task id is answered with 500 instead of 400
    [RavenFact(RavenTestCategory.BackupExportImport)]
    public async Task BackupHistory_ShouldRequireTheTaskIdWithTheFullBackupTime()
    {
        using var store = GetDocumentStore(new Options { RunInMemory = true });

        using var response = await GetFromEndpointAsync(store, "history", $"database={store.Database}&fullBackupCreatedAt=2026-01-01T00:00:00.0000000Z");

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
    }

    // #20: a backup stopped by a cancellation we didn't request (a direct upload timeout) is recorded as completed, without its error
    [RavenFact(RavenTestCategory.BackupExportImport)]
    public async Task BackupHistory_UnrequestedCancellationShouldBeRecordedAsFailed()
    {
        using var store = GetDocumentStore(new Options { RunInMemory = true });
        var config = Backup.CreateBackupConfiguration(NewDataPath(suffix: "BackupFolder"), fullBackupFrequency: "0 0 1 1 1");
        var result = await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config));
        await Cluster.WaitForRaftIndexToBeAppliedOnClusterNodesAsync(result.TaskId, nodes: [Server]);

        var database = await Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
        var canceledHold = new TaskCompletionSource<object>();
        canceledHold.SetCanceled();
        Server.ServerStore.ServerBackupRunner.ForTestingPurposesOnly().DatabaseTestingStuffInternals[database.Name] =
            new ServerBackupRunner.TestingStuffInternal { OnBackupTaskRunHoldBackupExecution = canceledHold };

        Server.ServerStore.ServerBackupRunner.StartBackupNowTask(database, result.TaskId, isFullBackup: true, database.Operations.GetNextOperationId());

        using (Server.ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
        {
            List<BlittableJsonReaderObject> groups = null;
            await WaitForValueAsync(async () =>
            {
                groups = await GetHistoryGroupsAsync(context, store, $"database={store.Database}");
                return groups.Count;
            }, 1);

            var fullBackup = GetObject(Assert.Single(groups), "FullBackup");
            Assert.NotNull(GetString(fullBackup, "Error"));
            Assert.Equal("Failed", GetString(fullBackup, "Status"));
        }

        List<BackupDecisionLogEntry> decisions = null;
        await WaitForValueAsync(() =>
        {
            decisions = Server.ServerStore.ServerBackupRunner.Decisions.GetDecisionLogDetails(new BackupDecisionLogFilter { Take = 100 }, afterEtag: null).Entries
                .Where(x => x.TaskId == result.TaskId)
                .ToList();
            return decisions.Any(x => x.Category == BackupDecisionCategory.Failed);
        }, true);

        Assert.Contains(decisions, x => x.Category == BackupDecisionCategory.Failed);
        Assert.DoesNotContain(decisions, x => x.Category == BackupDecisionCategory.Cancelled);
    }

    // #22: a canceled backup is not in the history
    [RavenFact(RavenTestCategory.BackupExportImport)]
    public async Task BackupHistory_ShouldStoreCanceledBackups()
    {
        using var store = GetDocumentStore(new Options { RunInMemory = true });
        var config = Backup.CreateBackupConfiguration(NewDataPath(suffix: "BackupFolder"), fullBackupFrequency: "0 0 1 1 1");
        var result = await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config));
        await Cluster.WaitForRaftIndexToBeAppliedOnClusterNodesAsync(result.TaskId, nodes: [Server]);

        var runner = Server.ServerStore.ServerBackupRunner;
        var database = await Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
        var hold = new TaskCompletionSource<object>(TaskCreationOptions.RunContinuationsAsynchronously);
        runner.ForTestingPurposesOnly().DatabaseTestingStuffInternals[database.Name] = new ServerBackupRunner.TestingStuffInternal { OnBackupTaskRunHoldBackupExecution = hold };

        try
        {
            var operationId = runner.StartBackupNowTask(database, result.TaskId, isFullBackup: true, database.Operations.GetNextOperationId());
            Assert.True(await WaitForValueAsync(() => Task.FromResult(BackupTestBase.GetBackupState(runner, store.Database, result.TaskId)?.RunningTask != null), true),
                "precondition: the backup is running");

            await database.Operations.KillOperationAsync(operationId, CancellationToken.None);
        }
        finally
        {
            hold.TrySetResult(null);
        }

        using (Server.ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
        {
            List<BlittableJsonReaderObject> groups = null;
            await WaitForValueAsync(async () =>
            {
                groups = await GetHistoryGroupsAsync(context, store, $"database={store.Database}");
                return groups.Count;
            }, 1);

            var fullBackup = GetObject(Assert.Single(groups), "FullBackup");
            Assert.Equal("Canceled", GetString(fullBackup, "Status"));
            Assert.Equal(Server.ServerStore.NodeTag, GetString(fullBackup, "NodeTag"));
        }
    }

    // #23: the details of a time with no backup are an empty success instead of not found
    [RavenFact(RavenTestCategory.BackupExportImport)]
    public async Task BackupResult_ShouldBeNotFoundForATimeWithNoBackup()
    {
        using var store = GetDocumentStore(new Options { RunInMemory = true });

        using var response = await GetFromEndpointAsync(store, "result", $"database={store.Database}&taskId=1&createdAt=2026-01-01T00:00:00.0000000Z");

        Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);
    }

    // #5: a backup that fails to start is not in the history, because its empty result fails the history write
    [RavenFact(RavenTestCategory.BackupExportImport)]
    public async Task BackupHistory_ShouldStoreBackupsThatFailedToStart()
    {
        var backupPath = NewDataPath(suffix: "BackupFolder");
        using var store = GetDocumentStore(new Options { RunInMemory = true });

        var config = Backup.CreateBackupConfiguration(backupPath, fullBackupFrequency: "0 0 1 1 1");
        var result = await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config));
        await Cluster.WaitForRaftIndexToBeAppliedOnClusterNodesAsync(result.TaskId, nodes: [Server]);

        await FailBackupStartAsync(Server, store, result.TaskId, isFullBackup: true);

        using (Server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
        {
            var backupHistory = GetBackupHistoryFromEndpoint(context, store, new BackupHistoryRequestParameters { DatabaseName = store.Database });

            var group = Assert.Single(backupHistory.Groups);
            Assert.Empty(group.IncrementalBackups);
            Assert.Equal(BackupKind.Full, group.FullBackup.BackupKind);
            Assert.Equal(Server.ServerStore.NodeTag, group.FullBackup.NodeTag);
            Assert.Contains(nameof(ServerBackupRunner.TestingStuffInternal.SimulateFailedBackupStart), group.FullBackup.Error);
        }
    }

    // #25: a failed history write is added to the backup result on every retry, with a wait of a second between retries
    [RavenFact(RavenTestCategory.BackupExportImport)]
    public async Task BackupHistory_FailedHistoryWriteShouldBeReportedOnce()
    {
        const string historyWriteError = "RavenDB-19358 history write error";
        using var server = GetNewServer(new ServerCreationOptions { RunInMemory = true });
        using var store = GetDocumentStore(new Options { Server = server, RunInMemory = true });
        server.ServerStore.DatabaseInfoCache.BackupHistoryStorage.ForTestingPurposesOnly().BeforeBackupHistoryUpdate = () => throw new InvalidOperationException(historyWriteError);

        var operation = await store.Maintenance.SendAsync(new BackupOperation(new BackupConfiguration
        {
            BackupType = BackupType.Backup,
            LocalSettings = new LocalSettings { FolderPath = NewDataPath(suffix: "BackupFolder") }
        }));
        var backupResult = (BackupResult)await operation.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

        Assert.Single(backupResult.Messages, message => message.Contains(historyWriteError));
    }

    // #6: an idle database's skipped incremental has no history row, and writing a row must not load the database
    [RavenFact(RavenTestCategory.BackupExportImport)]
    public async Task BackupHistory_ShouldStoreSkippedIncrementalBackupsOfIdleDatabasesWithoutWakingThem()
    {
        using var server = GetServerThatUnloadsIdleDatabases();
        using var store = GetDocumentStore(new Options { Server = server, RunInMemory = false });
        var (taskId, lastFullBackup) = await RunFullBackupAndWaitUntilIdleAsync(server, store);
        var reloads = CountDatabaseLoads(server, store.Database);
        var idleSince = DateTime.UtcNow;

        var skippedEntry = await WaitForIncrementalEntryAsync(server, store, taskId, idleSince);

        Assert.True(skippedEntry != null, "The skipped incremental backup of the idle database is not in the history");
        Assert.Equal(lastFullBackup, skippedEntry.LastFullBackup);
        Assert.Equal(server.ServerStore.NodeTag, skippedEntry.NodeTag);
        Assert.Null(skippedEntry.Error);
        Assert.Equal(0, reloads.Value);
        Assert.True(server.ServerStore.IdleDatabases.ContainsKey(store.Database), "The database should still be idle");
    }

    // #7 (7.2 bug): an idle database's skipped incremental keeps the previous status (IsFull, Error, NodeTag, duration)
    [RavenFact(RavenTestCategory.BackupExportImport)]
    public async Task IdleSkippedIncrementalBackupStatusShouldBeAnIncrementalOfThisNode()
    {
        using var server = GetServerThatUnloadsIdleDatabases();
        using var store = GetDocumentStore(new Options { Server = server, RunInMemory = false });
        var (taskId, _) = await RunFullBackupAndWaitUntilIdleAsync(server, store);
        var idleSince = DateTime.UtcNow;

        PeriodicBackupStatus skippedStatus = null;
        Assert.True(await WaitForValueAsync(() =>
        {
            skippedStatus = server.ServerStore.DatabaseInfoCache.BackupStatusStorage.GetBackupStatus(store.Database, taskId);
            return Task.FromResult(skippedStatus?.LastIncrementalBackupInternal >= idleSince);
        }, true, timeout: 150_000, interval: 1000), "The idle database's incremental backup was not skipped");

        Assert.False(skippedStatus.IsFull);
        Assert.Equal(server.ServerStore.NodeTag, skippedStatus.NodeTag);
        Assert.Null(skippedStatus.Error);
        Assert.Equal(0, skippedStatus.DurationInMs);
    }

    // #1: the history endpoint loads the database, so opening the page wakes an idle database
    [RavenFact(RavenTestCategory.BackupExportImport)]
    public async Task BackupHistory_ShouldNotWakeAnIdleDatabase()
    {
        using var server = GetServerThatUnloadsIdleDatabases();
        using var store = GetDocumentStore(new Options { Server = server, RunInMemory = false });
        await RunFullBackupAndWaitUntilIdleAsync(server, store);
        var loads = CountDatabaseLoads(server, store.Database);

        using var response = await GetFromEndpointAsync(store, "history", $"database={store.Database}");

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Equal(0, loads.Value);
        Assert.True(server.ServerStore.IdleDatabases.ContainsKey(store.Database), "The database should still be idle");
    }

    // #2: the history endpoints don't check that the user can access the database
    [RavenFact(RavenTestCategory.BackupExportImport | RavenTestCategory.Certificates)]
    public async Task BackupHistory_ShouldRequireAccessToTheDatabase()
    {
        var certificates = Certificates.SetupServerAuthentication();
        var allowedDatabase = GetDatabaseName();
        var otherDatabase = GetDatabaseName();
        var adminCertificate = Certificates.RegisterClientCertificate(certificates.ServerCertificateForCommunication.Value, certificates.ClientCertificate1.Value,
            new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin);
        var userCertificate = Certificates.RegisterClientCertificate(certificates.ServerCertificateForCommunication.Value, certificates.ClientCertificate2.Value,
            new Dictionary<string, DatabaseAccess> { [allowedDatabase] = DatabaseAccess.ReadWrite });

        using var otherStore = GetDocumentStore(new Options { AdminCertificate = adminCertificate, ClientCertificate = adminCertificate, ModifyDatabaseName = _ => otherDatabase });
        using var userStore = GetDocumentStore(new Options { AdminCertificate = adminCertificate, ClientCertificate = userCertificate, ModifyDatabaseName = _ => allowedDatabase });

        using var history = await GetFromEndpointAsync(userStore, "history", $"database={otherDatabase}");
        using var result = await GetFromEndpointAsync(userStore, "result", $"database={otherDatabase}&taskId=1&createdAt=2026-01-01T00:00:00.0000000Z");

        Assert.Equal(HttpStatusCode.Forbidden, history.StatusCode);
        Assert.Equal(HttpStatusCode.Forbidden, result.StatusCode);
    }

    // #3: the details of a sharded database's backup without a shard number return nothing instead of an error
    [RavenFact(RavenTestCategory.BackupExportImport | RavenTestCategory.Sharding)]
    public async Task BackupResult_ShouldRequireAShardNumberForAShardedDatabase()
    {
        using var store = Sharding.GetDocumentStore();

        using var response = await GetFromEndpointAsync(store, "result", $"database={store.Database}&taskId=1&createdAt=2026-01-01T00:00:00.0000000Z");

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
    }

    // #16: an undefined filter value is accepted, and a bad one is answered with 500
    [RavenFact(RavenTestCategory.BackupExportImport)]
    public async Task BackupHistory_ShouldRejectAnUndefinedTrigger()
    {
        using var store = GetDocumentStore(new Options { RunInMemory = true });

        using var undefined = await GetFromEndpointAsync(store, "history", $"database={store.Database}&trigger=7");
        using var invalid = await GetFromEndpointAsync(store, "history", $"database={store.Database}&trigger=NotATrigger");

        Assert.Equal(HttpStatusCode.BadRequest, undefined.StatusCode);
        Assert.Equal(HttpStatusCode.BadRequest, invalid.StatusCode);
    }

    [RavenFact(RavenTestCategory.BackupExportImport)]
    public async Task BackupHistory_ShouldRejectSeveralTriggersInOneValue()
    {
        using var store = GetDocumentStore(new Options { RunInMemory = true });

        using var response = await GetFromEndpointAsync(store, "history", $"database={store.Database}&trigger=Scheduled,BackupNow");

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
    }

    // #8: a deleted task's history is hidden and its name is lost
    [RavenFact(RavenTestCategory.BackupExportImport)]
    public async Task BackupHistory_ShouldShowTheHistoryOfADeletedTaskWithItsName()
    {
        const string taskName = "Deleted backup task";
        var backupPath = NewDataPath(suffix: "BackupFolder");
        using var store = GetDocumentStore(new Options { RunInMemory = true });

        var config = Backup.CreateBackupConfiguration(backupPath, fullBackupFrequency: "0 0 1 1 1", name: taskName);
        var result = await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config));
        await Cluster.WaitForRaftIndexToBeAppliedOnClusterNodesAsync(result.TaskId, nodes: [Server]);
        await RunBackupsAccordingPlan([BackupKind.Full, BackupKind.Incremental], Server, result.TaskId, store);

        var deleteResult = await store.Maintenance.SendAsync(new DeleteOngoingTaskOperation(result.TaskId, OngoingTaskType.Backup));
        await Cluster.WaitForRaftIndexToBeAppliedOnClusterNodesAsync(deleteResult.RaftCommandIndex, nodes: [Server]);

        using (Server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
        {
            var backupHistory = GetBackupHistoryFromEndpoint(context, store, new BackupHistoryRequestParameters { DatabaseName = store.Database });

            var group = Assert.Single(backupHistory.Groups);
            Assert.Equal(result.TaskId, group.TaskId);
            Assert.Equal(taskName, group.TaskName);
        }
    }

    // #9: one-time backups are stored in the history but never shown
    [RavenFact(RavenTestCategory.BackupExportImport)]
    public async Task BackupHistory_ShouldShowOneTimeBackups()
    {
        var backupPath = NewDataPath(suffix: "BackupFolder");
        using var store = GetDocumentStore(new Options { RunInMemory = true });

        await RunOneTimeBackupAsync(store, backupPath);
        await RunOneTimeBackupAsync(store, backupPath);

        using (Server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
        {
            var backupHistory = GetBackupHistoryFromEndpoint(context, store, new BackupHistoryRequestParameters { DatabaseName = store.Database });

            Assert.Equal(2, backupHistory.Groups.Count);
            Assert.All(backupHistory.Groups, group =>
            {
                Assert.Equal(0, group.TaskId);
                Assert.StartsWith("One Time Backup #", group.TaskName);
                Assert.Equal(BackupKind.Full, group.FullBackup.BackupKind);
                Assert.Null(group.FullBackup.Error);
            });
            Assert.NotEqual(backupHistory.Groups[0].TaskName, backupHistory.Groups[1].TaskName);
        }
    }

    // #10: a failed one-time backup never reaches the history
    [RavenFact(RavenTestCategory.BackupExportImport)]
    public async Task BackupHistory_ShouldStoreFailedOneTimeBackups()
    {
        var backupPath = NewDataPath(suffix: "BackupFolder");
        using var store = GetDocumentStore(new Options { RunInMemory = true });

        var documentDatabase = await Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
        Server.ServerStore.ServerBackupRunner.ForTestingPurposesOnly().DatabaseTestingStuffInternals[documentDatabase.Name] =
            new ServerBackupRunner.TestingStuffInternal { SimulateFailedBackup = true };

        var operation = await store.Maintenance.SendAsync(new BackupOperation(new BackupConfiguration
        {
            BackupType = BackupType.Backup,
            LocalSettings = new LocalSettings { FolderPath = backupPath }
        }));
        await Assert.ThrowsAnyAsync<Exception>(() => operation.WaitForCompletionAsync(TimeSpan.FromSeconds(30)));

        using (Server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
        {
            var backupHistory = GetBackupHistoryFromEndpoint(context, store, new BackupHistoryRequestParameters { DatabaseName = store.Database });

            var group = Assert.Single(backupHistory.Groups);
            Assert.Equal(0, group.TaskId);
            Assert.Contains(nameof(ServerBackupRunner.TestingStuffInternal.SimulateFailedBackup), group.FullBackup.Error);
        }
    }

    // #11: a history row doesn't say what started the backup, or whether it completed, failed or was skipped
    [RavenFact(RavenTestCategory.BackupExportImport)]
    public async Task BackupHistory_ShouldStoreTheTriggerAndTheStatus()
    {
        var backupPath = NewDataPath(suffix: "BackupFolder");
        using var store = GetDocumentStore(new Options { RunInMemory = true });
        await StoreUserAsync(store, "users/1");

        var config = Backup.CreateBackupConfiguration(backupPath, fullBackupFrequency: "0 0 1 1 1");
        var result = await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config));
        await Cluster.WaitForRaftIndexToBeAppliedOnClusterNodesAsync(result.TaskId, nodes: [Server]);

        await Backup.RunBackupAsync(Server, result.TaskId, store, isFullBackup: true);
        await StoreUserAsync(store, "users/2");
        await Backup.RunBackupAsync(Server, result.TaskId, store, isFullBackup: false);
        await Backup.RunBackupAsync(Server, result.TaskId, store, isFullBackup: false);
        await RunOneTimeBackupAsync(store, backupPath);

        using (Server.ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
        {
            var groups = await GetHistoryGroupsAsync(context, store, $"database={store.Database}");

            var taskGroup = groups.Single(group => GetLong(group, "TaskId") == result.TaskId);
            var fullBackup = GetObject(taskGroup, "FullBackup");
            Assert.Equal("BackupNow", GetString(fullBackup, "Trigger"));
            Assert.Equal("Completed", GetString(fullBackup, "Status"));

            var incrementals = GetObjects(taskGroup, "IncrementalBackups").OrderBy(entry => GetString(entry, "CreatedAt")).ToList();
            Assert.Equal(["Completed", "Skipped"], incrementals.Select(entry => GetString(entry, "Status")));
            Assert.All(incrementals, entry => Assert.Equal("BackupNow", GetString(entry, "Trigger")));

            var oneTimeGroup = groups.Single(group => GetLong(group, "TaskId") == 0);
            Assert.Equal("OneTime", GetString(GetObject(oneTimeGroup, "FullBackup"), "Trigger"));
        }
    }

    // #15: database names in other languages share key bytes, so deleting one database's history deletes the other's too
    [RavenFact(RavenTestCategory.BackupExportImport)]
    public async Task BackupHistory_DeletingADatabaseShouldKeepTheHistoryOfADatabaseWithASimilarNonEnglishName()
    {
        var backupPath = NewDataPath(suffix: "BackupFolder");
        var baseName = GetDatabaseName();
        using var server = GetNewServer(new ServerCreationOptions { RunInMemory = true });
        using var hebrewStore = GetDocumentStore(new Options { Server = server, RunInMemory = true, ModifyDatabaseName = _ => baseName + "ס" });
        using var spanishStore = GetDocumentStore(new Options { Server = server, RunInMemory = true, ModifyDatabaseName = _ => baseName + "á" });

        foreach (var store in new[] { hebrewStore, spanishStore })
        {
            var config = Backup.CreateBackupConfiguration(backupPath, fullBackupFrequency: "0 0 1 1 1");
            var result = await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config));
            await Cluster.WaitForRaftIndexToBeAppliedOnClusterNodesAsync(result.TaskId, nodes: [server]);
            await Backup.RunBackupAsync(server, result.TaskId, store, isFullBackup: true);
        }
        Assert.Equal(2, CountStoredEntries(server));

        await hebrewStore.Maintenance.Server.SendAsync(new DeleteDatabasesOperation(hebrewStore.Database, hardDelete: true));

        await AssertWaitForValueAsync(() => Task.FromResult(CountStoredEntries(server)), 1L);
        using (server.ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            Assert.Single(await GetHistoryGroupsAsync(context, spanishStore, $"database={Uri.EscapeDataString(spanishStore.Database)}"));
    }

    // #21: a backup's details and its group are found only by the .NET ticks of its time, which the history doesn't return
    [RavenFact(RavenTestCategory.BackupExportImport)]
    public async Task BackupHistory_ShouldFindTheDetailsAndTheGroupByTheTimeInTheHistory()
    {
        var backupPath = NewDataPath(suffix: "BackupFolder");
        using var store = GetDocumentStore(new Options { RunInMemory = true });

        var config = Backup.CreateBackupConfiguration(backupPath, fullBackupFrequency: "0 0 1 1 1");
        var taskId = await Backup.UpdateConfigAndRunBackupAsync(Server, config, store);

        using (Server.ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
        {
            var createdAt = GetString(GetObject(Assert.Single(await GetHistoryGroupsAsync(context, store, $"database={store.Database}")), "FullBackup"), "CreatedAt");

            using var details = await GetFromEndpointAsync(store, "result", $"database={store.Database}&taskId={taskId}&createdAt={Uri.EscapeDataString(createdAt)}");
            Assert.Equal(HttpStatusCode.OK, details.StatusCode);
            Assert.Contains("\"Documents\"", await details.Content.ReadAsStringAsync());

            var group = Assert.Single(await GetHistoryGroupsAsync(context, store, $"database={store.Database}&taskId={taskId}&fullBackupCreatedAt={Uri.EscapeDataString(createdAt)}"));
            Assert.Equal(createdAt, GetString(GetObject(group, "FullBackup"), "CreatedAt"));
        }
    }

    // #37: the history doesn't show all the nodes together by default
    [RavenFact(RavenTestCategory.BackupExportImport | RavenTestCategory.Cluster)]
    public async Task BackupHistory_ShouldShowTheHistoryOfAllNodes()
    {
        var backupPath = NewDataPath(suffix: "BackupFolder");
        var (nodes, leader) = await CreateRaftCluster(2);
        using var store = GetDocumentStore(new Options { Server = leader, ReplicationFactor = 2 });

        var queriedNode = nodes.Single(node => string.Equals(node.WebUrl, store.Urls.First(), StringComparison.OrdinalIgnoreCase));
        var backupNode = nodes.Single(node => node != queriedNode);
        var backupNodeTag = backupNode.ServerStore.NodeTag;

        var config = Backup.CreateBackupConfiguration(backupPath, fullBackupFrequency: "0 0 1 1 1", mentorNode: backupNodeTag, pinToMentorNode: true);
        var taskId = await Backup.UpdateConfigAndRunBackupAsync(backupNode, config, store);

        using (queriedNode.ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
        {
            var group = Assert.Single(await GetHistoryGroupsAsync(context, store, $"database={store.Database}"));
            Assert.Equal(taskId, GetLong(group, "TaskId"));
            Assert.Equal(backupNodeTag, GetString(GetObject(group, "FullBackup"), "NodeTag"));

            var createdAt = GetString(GetObject(group, "FullBackup"), "CreatedAt");
            using var details = await GetFromEndpointAsync(store, "result", $"database={store.Database}&taskId={taskId}&createdAt={Uri.EscapeDataString(createdAt)}&nodeTag={backupNodeTag}");
            Assert.Equal(HttpStatusCode.OK, details.StatusCode);
            Assert.Contains("\"Documents\"", await details.Content.ReadAsStringAsync());
        }
    }

    // #37: the history doesn't show all the nodes together by default
    [RavenFact(RavenTestCategory.BackupExportImport | RavenTestCategory.Cluster)]
    public async Task BackupHistory_ShouldJoinAGroupSplitBetweenNodesAndNameTheNodesThatDidNotAnswer()
    {
        var (nodes, leader) = await CreateRaftCluster(3);
        using var store = GetDocumentStore(new Options { Server = leader, ReplicationFactor = 3 });

        var followers = nodes.Where(node => node != leader).ToList();
        var incrementalNode = followers[0];
        var downNode = followers[1];

        var fullBackup = DateTime.UtcNow.AddHours(-1);
        await StoreHistoryEntryAsync(leader, store.Database, taskId: 1, fullBackup, lastFullBackup: fullBackup);
        await StoreHistoryEntryAsync(incrementalNode, store.Database, taskId: 1, fullBackup.AddMinutes(10), lastFullBackup: fullBackup, isFull: false);
        await StoreHistoryEntryAsync(incrementalNode, store.Database, taskId: 1, fullBackup.AddMinutes(20), lastFullBackup: fullBackup, isFull: false);

        var downNodeTag = downNode.ServerStore.NodeTag;
        await DisposeServerAndWaitForFinishOfDisposalAsync(downNode);

        using (leader.ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
        {
            var history = await GetHistoryJsonAsync(context, store, $"database={store.Database}");
            var group = Assert.Single(GetObjects(history, "Groups"));

            Assert.True(GetBool(group, "FullBackupInHistory"));
            Assert.Equal(leader.ServerStore.NodeTag, GetString(GetObject(group, "FullBackup"), "NodeTag"));
            Assert.Equal(2, GetLong(group, "IncrementalBackupsCount"));
            Assert.Equal(2, GetLong(GetObject(group, "IncrementalBackupsByStatus"), "Completed"));
            var incrementals = GetObjects(group, "IncrementalBackups");
            Assert.Equal(2, incrementals.Count);
            Assert.All(incrementals, incremental => Assert.Equal(incrementalNode.ServerStore.NodeTag, GetString(incremental, "NodeTag")));
            var unreachableNode = Assert.Single(GetObjects(history, "UnreachableNodes"));
            Assert.Equal(downNodeTag, GetString(unreachableNode, "NodeTag"));
            Assert.False(string.IsNullOrEmpty(GetString(unreachableNode, "Error")));

            var from = Uri.EscapeDataString(fullBackup.AddMinutes(5).ToString("O"));
            var filteredGroup = Assert.Single(await GetHistoryGroupsAsync(context, store, $"database={store.Database}&from={from}"));
            Assert.True(GetBool(filteredGroup, "FullBackupInHistory"));
            Assert.Equal(leader.ServerStore.NodeTag, GetString(GetObject(filteredGroup, "FullBackup"), "NodeTag"));

            using (var incrementalNodeStore = new DocumentStore { Urls = [incrementalNode.WebUrl], Database = store.Database, Conventions = { DisableTopologyUpdates = true } })
            {
                incrementalNodeStore.Initialize();

                var groupFromTheOtherNode = Assert.Single(await GetHistoryGroupsAsync(context, incrementalNodeStore, $"database={store.Database}&from={from}"));
                Assert.True(GetBool(groupFromTheOtherNode, "FullBackupInHistory"));
                Assert.Equal(leader.ServerStore.NodeTag, GetString(GetObject(groupFromTheOtherNode, "FullBackup"), "NodeTag"));
            }

            var runs = await GetHistoryJsonAsync(context, store, $"database={store.Database}&groupBy=None&pageSize=1");
            Assert.Single(GetObjects(runs, "Runs"));
            Assert.Equal(3, GetLong(runs, "TotalResults"));
        }
    }

    [RavenFact(RavenTestCategory.BackupExportImport | RavenTestCategory.Cluster)]
    public async Task BackupHistory_SortByDurationShouldUseTheFullBackupOfAnotherNode()
    {
        var (nodes, leader) = await CreateRaftCluster(2);
        using var store = GetDocumentStore(new Options { Server = leader, ReplicationFactor = 2 });
        var otherNode = nodes.Single(node => node != leader);

        var fullBackup = DateTime.UtcNow.AddHours(-1);
        await StoreHistoryEntryAsync(leader, store.Database, taskId: 1, fullBackup, lastFullBackup: fullBackup, durationInMs: 300);
        await StoreHistoryEntryAsync(otherNode, store.Database, taskId: 1, fullBackup.AddMinutes(10), lastFullBackup: fullBackup, isFull: false);
        await StoreHistoryEntryAsync(otherNode, store.Database, taskId: 2, fullBackup.AddMinutes(20), lastFullBackup: fullBackup.AddMinutes(20), durationInMs: 100);

        using (var otherNodeStore = new DocumentStore { Urls = [otherNode.WebUrl], Database = store.Database, Conventions = { DisableTopologyUpdates = true } })
        using (otherNode.ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
        {
            otherNodeStore.Initialize();

            var from = Uri.EscapeDataString(fullBackup.AddMinutes(5).ToString("O"));
            var history = await GetHistoryJsonAsync(context, otherNodeStore, $"database={store.Database}&from={from}&sortBy=Duration&descending=false&pageSize=1");

            var group = Assert.Single(GetObjects(history, "Groups"));
            Assert.Equal(2, GetLong(group, "TaskId"));
            Assert.Equal(2, GetLong(history, "TotalResults"));
        }
    }

    [RavenFact(RavenTestCategory.BackupExportImport | RavenTestCategory.Sharding | RavenTestCategory.Cluster)]
    public async Task BackupHistory_ShouldShowTheShardsOfEveryNode()
    {
        var (nodes, leader) = await CreateRaftCluster(2);
        using var store = Sharding.GetDocumentStore(new Options { Server = leader });

        var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
        var shardNumbers = record.Sharding.Shards.Keys.OrderBy(shardNumber => shardNumber).Take(2).ToList();
        var otherNode = nodes.Single(node => node != leader);

        var createdAt = DateTime.UtcNow.AddHours(-1);
        await StoreHistoryEntryAsync(leader, ShardHelper.ToShardName(store.Database, shardNumbers[0]), taskId: 1, createdAt, lastFullBackup: createdAt);
        await StoreHistoryEntryAsync(leader, ShardHelper.ToShardName(store.Database, shardNumbers[0]), taskId: 1, createdAt.AddMinutes(10), lastFullBackup: createdAt, isFull: false);
        await StoreHistoryEntryAsync(otherNode, ShardHelper.ToShardName(store.Database, shardNumbers[1]), taskId: 1, createdAt, lastFullBackup: createdAt);
        await StoreHistoryEntryAsync(leader, ShardHelper.ToShardName(store.Database, shardNumbers[1]), taskId: 1, createdAt.AddMinutes(10), lastFullBackup: createdAt, isFull: false);

        using (leader.ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
        {
            var groups = (await GetHistoryGroupsAsync(context, store, $"database={store.Database}")).OrderBy(group => GetLong(group, "ShardNumber")).ToList();

            Assert.Equal(shardNumbers, groups.Select(group => (int)GetLong(group, "ShardNumber")).ToList());
            Assert.Equal([leader.ServerStore.NodeTag, otherNode.ServerStore.NodeTag], groups.Select(group => GetString(GetObject(group, "FullBackup"), "NodeTag")).ToList());

            using (var leaderStore = new DocumentStore { Urls = [leader.WebUrl], Database = store.Database, Conventions = { DisableTopologyUpdates = true } })
            {
                leaderStore.Initialize();

                var from = Uri.EscapeDataString(createdAt.AddMinutes(5).ToString("O"));
                var shardGroups = (await GetHistoryGroupsAsync(context, leaderStore, $"database={store.Database}&from={from}")).OrderBy(group => GetLong(group, "ShardNumber")).ToList();
                Assert.Equal(shardNumbers, shardGroups.Select(group => (int)GetLong(group, "ShardNumber")).ToList());
                Assert.All(shardGroups, group => Assert.True(GetBool(group, "FullBackupInHistory")));
                Assert.Equal([leader.ServerStore.NodeTag, otherNode.ServerStore.NodeTag], shardGroups.Select(group => GetString(GetObject(group, "FullBackup"), "NodeTag")).ToList());
            }
        }
    }

    [RavenFact(RavenTestCategory.BackupExportImport)]
    public async Task BackupHistory_ShouldRejectABadSortGroupOrDate()
    {
        using var store = GetDocumentStore(new Options { RunInMemory = true });

        foreach (var query in new[] { "sortBy=NotASort", "groupBy=7", "sortBy=Date&sortBy=Node", "from=not-a-date" })
        {
            using var response = await GetFromEndpointAsync(store, "history", $"database={store.Database}&{query}");
            Assert.True(response.StatusCode == HttpStatusCode.BadRequest, $"{query} returned {(int)response.StatusCode}");
        }
    }

    // #13: a sharded database's history is empty, and shards that back up at the same time are mixed
    [RavenFact(RavenTestCategory.BackupExportImport | RavenTestCategory.Sharding)]
    public async Task BackupHistory_ShouldShowTheHistoryOfEveryShard()
    {
        var backupPath = NewDataPath(suffix: "BackupFolder");
        using var store = Sharding.GetDocumentStore();

        var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
        var shardNumbers = record.Sharding.Shards.Keys.OrderBy(shardNumber => shardNumber).ToList();

        var config = Backup.CreateBackupConfiguration(backupPath, fullBackupFrequency: "0 0 1 1 1");
        var (taskId, operation) = await Sharding.Backup.UpdateConfigurationAndRunBackupAsync(Server, store, config, isFullBackup: true);
        await operation.WaitForCompletionAsync(TimeSpan.FromSeconds(60));

        using (Server.ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
        {
            var groups = await GetHistoryGroupsAsync(context, store, $"database={store.Database}");

            Assert.Equal(shardNumbers, groups.Select(group => (int)GetLong(group, "ShardNumber")).OrderBy(shardNumber => shardNumber).ToList());
            Assert.All(groups, group => Assert.Equal(taskId, GetLong(group, "TaskId")));

            var oneShard = await GetHistoryGroupsAsync(context, store, $"database={store.Database}&shardNumber={shardNumbers[0]}");
            Assert.Equal(shardNumbers[0], (int)GetLong(Assert.Single(oneShard), "ShardNumber"));
        }
    }

    // #32: the history can't be filtered on the server, and there are no counts for the filter bar
    [RavenFact(RavenTestCategory.BackupExportImport)]
    public async Task BackupHistory_ShouldFilterOnTheServerAndReturnCounts()
    {
        var backupPath = NewDataPath(suffix: "BackupFolder");
        using var store = GetDocumentStore(new Options { RunInMemory = true });

        await RunOneTimeBackupAsync(store, backupPath);

        var config = Backup.CreateBackupConfiguration(backupPath, fullBackupFrequency: "0 0 1 1 1");
        var result = await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config));
        await Cluster.WaitForRaftIndexToBeAppliedOnClusterNodesAsync(result.TaskId, nodes: [Server]);
        await Backup.RunBackupAsync(Server, result.TaskId, store, isFullBackup: true);
        await Backup.RunBackupAsync(Server, result.TaskId, store, isFullBackup: false);

        using (Server.ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
        {
            var backupNow = await GetHistoryJsonAsync(context, store, $"database={store.Database}&trigger=BackupNow");
            var group = Assert.Single(GetObjects(backupNow, "Groups"));
            Assert.Equal(result.TaskId, GetLong(group, "TaskId"));
            Assert.Equal(1, GetLong(group, "IncrementalBackupsCount"));

            var counts = GetObject(backupNow, "Counts");
            Assert.Equal(3, GetLong(counts, "Total"));
            Assert.Equal(2, GetLong(GetObject(counts, "ByTrigger"), "BackupNow"));
            Assert.Equal(1, GetLong(GetObject(counts, "ByTrigger"), "OneTime"));
            Assert.Equal(3, GetLong(GetObject(counts, "ByBackupType"), "Backup"));

            var future = await GetHistoryJsonAsync(context, store, $"database={store.Database}&from={DateTime.UtcNow.AddDays(1):O}");
            Assert.Empty(GetObjects(future, "Groups"));
            Assert.Equal(0, GetLong(GetObject(future, "Counts"), "Total"));
        }
    }

    // #33: the history has no paging, every group is returned at once
    [RavenFact(RavenTestCategory.BackupExportImport)]
    public async Task BackupHistory_ShouldPageTheGroups()
    {
        var backupPath = NewDataPath(suffix: "BackupFolder");
        using var store = GetDocumentStore(new Options { RunInMemory = true });

        var config = Backup.CreateBackupConfiguration(backupPath, fullBackupFrequency: "0 0 1 1 1");
        var result = await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config));
        await Cluster.WaitForRaftIndexToBeAppliedOnClusterNodesAsync(result.TaskId, nodes: [Server]);
        await RunBackupsAccordingPlan([BackupKind.Full, BackupKind.Full, BackupKind.Full], Server, result.TaskId, store);

        using (Server.ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
        {
            var firstPage = await GetHistoryJsonAsync(context, store, $"database={store.Database}&pageSize=2");
            var secondPage = await GetHistoryJsonAsync(context, store, $"database={store.Database}&start=2&pageSize=2");

            Assert.Equal(2, GetObjects(firstPage, "Groups").Count);
            Assert.Single(GetObjects(secondPage, "Groups"));
            Assert.Equal(3, GetLong(firstPage, "TotalResults"));
        }
    }

    // #35: the history can't be sorted, groups always come newest first
    [RavenFact(RavenTestCategory.BackupExportImport)]
    public async Task BackupHistory_ShouldSortTheGroups()
    {
        using var server = GetNewServer(new ServerCreationOptions { RunInMemory = true });
        using var store = GetDocumentStore(new Options { Server = server, RunInMemory = true });

        var start = DateTime.UtcNow.AddHours(-1);
        await StoreHistoryEntryAsync(server, store.Database, taskId: 1, start, lastFullBackup: start, taskName: "Beta", durationInMs: 300);
        await StoreHistoryEntryAsync(server, store.Database, taskId: 2, start.AddMinutes(1), lastFullBackup: start.AddMinutes(1), taskName: "Alpha", durationInMs: 100);
        await StoreHistoryEntryAsync(server, store.Database, taskId: 3, start.AddMinutes(2), lastFullBackup: start.AddMinutes(2), taskName: "Gamma", durationInMs: 200);

        using (server.ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
        {
            Assert.Equal(["Gamma", "Alpha", "Beta"], await GetTaskNamesAsync(context, store, ""));
            Assert.Equal(["Alpha", "Beta", "Gamma"], await GetTaskNamesAsync(context, store, "&sortBy=TaskName&descending=false"));
            Assert.Equal(["Beta", "Gamma", "Alpha"], await GetTaskNamesAsync(context, store, "&sortBy=Duration"));
            Assert.Equal(["Beta", "Alpha", "Gamma"], await GetTaskNamesAsync(context, store, "&sortBy=Date&descending=false"));
        }

        static async Task<List<string>> GetTaskNamesAsync(JsonOperationContext context, DocumentStore store, string query) =>
            (await GetHistoryGroupsAsync(context, store, $"database={store.Database}{query}")).Select(group => GetString(group, "TaskName")).ToList();
    }

    // #36: every run can't be shown as its own row, the history is always grouped by full backup
    [RavenFact(RavenTestCategory.BackupExportImport)]
    public async Task BackupHistory_ShouldListEveryRunWhenNotGrouped()
    {
        using var server = GetNewServer(new ServerCreationOptions { RunInMemory = true });
        using var store = GetDocumentStore(new Options { Server = server, RunInMemory = true });

        var start = DateTime.UtcNow.AddHours(-1);
        await StoreHistoryEntryAsync(server, store.Database, taskId: 1, start, lastFullBackup: start);
        await StoreHistoryEntryAsync(server, store.Database, taskId: 1, start.AddMinutes(1), lastFullBackup: start, isFull: false);
        await StoreHistoryEntryAsync(server, store.Database, taskId: 1, start.AddMinutes(2), lastFullBackup: start, isFull: false);

        using (server.ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
        {
            var history = await GetHistoryJsonAsync(context, store, $"database={store.Database}&groupBy=None&pageSize=2");
            var runs = GetObjects(history, "Runs");

            Assert.Equal(3, GetLong(history, "TotalResults"));
            Assert.Empty(GetObjects(history, "Groups"));
            Assert.Equal(2, runs.Count);
            Assert.All(runs, run => Assert.Equal("Incremental", GetString(run, "BackupKind")));
            Assert.All(runs, run => Assert.Equal(1, GetLong(run, "TaskId")));
            Assert.True(GetDateTime(runs[0], "CreatedAt") > GetDateTime(runs[1], "CreatedAt"));
        }
    }

    // #17: a group whose full backup is not in this node's history disappears without incrementals, and with them it shows a made-up full backup as if it were stored
    [RavenFact(RavenTestCategory.BackupExportImport)]
    public async Task BackupHistory_ShouldShowAGroupWhoseFullBackupIsNotInThisNodesHistory()
    {
        using var store = GetDocumentStore(new Options { RunInMemory = true });
        var lastFullBackup = DateTime.UtcNow.AddHours(-1);
        var incremental = new PeriodicBackupStatus
        {
            TaskId = 1,
            IsFull = false,
            BackupType = BackupType.Backup,
            NodeTag = Server.ServerStore.NodeTag,
            LastFullBackup = lastFullBackup,
            LastFullBackupInternal = lastFullBackup,
            LastIncrementalBackupInternal = DateTime.UtcNow,
            DurationInMs = 10
        };
        await Server.ServerStore.Engine.TxMerger.Enqueue(new UpdateBackupHistoryCommand([new BackupHistoryUpdate(store.Database, incremental.TaskId, new BackupHistoryEntry(incremental, "RavenDB-19358", BackupTrigger.Scheduled, skipped: false), new BackupResult())]));

        using (Server.ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
        {
            var withIncrementals = Assert.Single(await GetHistoryGroupsAsync(context, store, $"database={store.Database}&taskId=1"));
            Assert.False(GetBool(withIncrementals, "FullBackupInHistory"));
            Assert.Null(GetString(GetObject(withIncrementals, "FullBackup"), "Status"));
            Assert.Null(GetString(GetObject(withIncrementals, "FullBackup"), "NodeTag"));

            var withoutIncrementals = Assert.Single(await GetHistoryGroupsAsync(context, store, $"database={store.Database}&taskId=1&includeIncrementals=false"));
            Assert.Equal(1, GetLong(withoutIncrementals, "IncrementalBackupsCount"));
        }
    }

    // #14: the history is not deleted when the database is deleted
    [RavenFact(RavenTestCategory.BackupExportImport)]
    public async Task BackupHistory_ShouldBeDeletedWithTheDatabase()
    {
        var backupPath = NewDataPath(suffix: "BackupFolder");
        using var server = GetNewServer(new ServerCreationOptions { RunInMemory = true });
        using var store = GetDocumentStore(new Options { Server = server, RunInMemory = true });

        var config = Backup.CreateBackupConfiguration(backupPath, fullBackupFrequency: "0 0 1 1 1");
        var result = await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config));
        await Cluster.WaitForRaftIndexToBeAppliedOnClusterNodesAsync(result.TaskId, nodes: [server]);
        await RunBackupsAccordingPlan([BackupKind.Full, BackupKind.Incremental], server, result.TaskId, store);
        Assert.Equal(2, CountStoredEntries(server));

        await store.Maintenance.Server.SendAsync(new DeleteDatabasesOperation(store.Database, hardDelete: true));

        await AssertWaitForValueAsync(() => Task.FromResult(CountStoredEntries(server)), 0L);
        Assert.Equal(0, CountStoredDetails(server));
    }

    // Coverage (#14): a node that only leaves the database group keeps its history, it is deleted only with the whole database (Pawel, RavenDB-25376)
    [RavenFact(RavenTestCategory.BackupExportImport | RavenTestCategory.Cluster)]
    public async Task BackupHistory_ShouldBeKeptOnANodeRemovedFromTheDatabaseGroup()
    {
        var backupPath = NewDataPath(suffix: "BackupFolder");
        var (nodes, leader) = await CreateRaftCluster(2);
        using var store = GetDocumentStore(new Options { Server = leader, ReplicationFactor = 2 });

        var backupNode = nodes.First(node => node != leader);
        var backupNodeTag = backupNode.ServerStore.NodeTag;

        var config = Backup.CreateBackupConfiguration(backupPath, fullBackupFrequency: "0 0 1 1 1", mentorNode: backupNodeTag, pinToMentorNode: true);
        await Backup.UpdateConfigAndRunBackupAsync(backupNode, config, store);
        Assert.Equal(1, CountStoredEntries(backupNode));

        await store.Maintenance.Server.SendAsync(new DeleteDatabasesOperation(store.Database, hardDelete: true, fromNode: backupNodeTag));

        Assert.True(await WaitForValueAsync(async () =>
        {
            var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
            return record.Topology.AllNodes.Contains(backupNodeTag) == false && (record.DeletionInProgress == null || record.DeletionInProgress.ContainsKey(backupNodeTag) == false);
        }, expectedVal: true), "precondition: the node should leave the database group");

        Assert.Equal(1, CountStoredEntries(backupNode));
    }

    // #26 and #27: old rows go only after a successful full backup, and the group of the newest full backup grows forever
    [RavenFact(RavenTestCategory.BackupExportImport)]
    public async Task BackupHistoryCleaner_ShouldRemoveExpiredEntriesAndKeepTheNewestSuccessfulFullBackupOfEachTask()
    {
        using var server = GetNewServer(new ServerCreationOptions { RunInMemory = true });
        using var store = GetDocumentStore(new Options { Server = server, RunInMemory = true });
        server.ServerStore.BackupHistoryCleaner.ForTestingPurposesOnly().RetentionPeriod = TimeSpan.FromHours(1);

        var config = Backup.CreateBackupConfiguration(NewDataPath(suffix: "BackupFolder"), fullBackupFrequency: "0 0 1 1 1");
        var result = await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config));
        await Cluster.WaitForRaftIndexToBeAppliedOnClusterNodesAsync(result.TaskId, nodes: [server]);

        var start = DateTime.UtcNow;
        var newestSuccessfulFullBackup = start.AddMinutes(2);
        await StoreHistoryEntryAsync(server, store.Database, result.TaskId, start, lastFullBackup: start);
        await StoreHistoryEntryAsync(server, store.Database, result.TaskId, start.AddMinutes(1), lastFullBackup: start, isFull: false);
        await StoreHistoryEntryAsync(server, store.Database, result.TaskId, newestSuccessfulFullBackup, lastFullBackup: newestSuccessfulFullBackup);
        await StoreHistoryEntryAsync(server, store.Database, result.TaskId, start.AddMinutes(3), lastFullBackup: newestSuccessfulFullBackup, isFull: false);
        await StoreHistoryEntryAsync(server, store.Database, result.TaskId, start.AddMinutes(4), lastFullBackup: newestSuccessfulFullBackup, error: "RavenDB-19358 failed full backup");

        Assert.Equal(0, await server.ServerStore.BackupHistoryCleaner.CleanupAsync(start));
        Assert.Equal(4, await server.ServerStore.BackupHistoryCleaner.CleanupAsync(start.AddHours(2)));

        using (server.ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
        {
            var group = Assert.Single(await GetHistoryGroupsAsync(context, store, $"database={store.Database}"));
            var fullBackup = GetObject(group, "FullBackup");
            Assert.Equal(newestSuccessfulFullBackup, DateTime.Parse(GetString(fullBackup, "CreatedAt"), CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind));
            Assert.Equal(0, GetLong(group, "IncrementalBackupsCount"));
        }

        Assert.Equal(1, CountStoredDetails(server));
    }

    [RavenFact(RavenTestCategory.BackupExportImport)]
    public async Task BackupHistoryCleaner_ShouldNotKeepACanceledFullBackupAsTheNewestSuccessfulOne()
    {
        using var server = GetNewServer(new ServerCreationOptions { RunInMemory = true });
        using var store = GetDocumentStore(new Options { Server = server, RunInMemory = true });
        server.ServerStore.BackupHistoryCleaner.ForTestingPurposesOnly().RetentionPeriod = TimeSpan.FromHours(1);

        var config = Backup.CreateBackupConfiguration(NewDataPath(suffix: "BackupFolder"), fullBackupFrequency: "0 0 1 1 1");
        var result = await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config));
        await Cluster.WaitForRaftIndexToBeAppliedOnClusterNodesAsync(result.TaskId, nodes: [server]);

        var start = DateTime.UtcNow;
        await StoreHistoryEntryAsync(server, store.Database, result.TaskId, start, lastFullBackup: start);

        var status = new PeriodicBackupStatus { TaskId = result.TaskId, BackupType = BackupType.Backup, LastFullBackup = start };
        var canceled = BackupHistoryEntry.CreateForCanceledBackup(status, start.AddMinutes(1), isFullBackup: true, durationInMs: 10, server.ServerStore.NodeTag, "RavenDB-19358", BackupTrigger.Scheduled);
        await server.ServerStore.Engine.TxMerger.Enqueue(new UpdateBackupHistoryCommand([new BackupHistoryUpdate(store.Database, result.TaskId, canceled, new BackupResult())]));

        Assert.Equal(1, await server.ServerStore.BackupHistoryCleaner.CleanupAsync(start.AddHours(2)));

        using (server.ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
        {
            var fullBackup = GetObject(Assert.Single(await GetHistoryGroupsAsync(context, store, $"database={store.Database}")), "FullBackup");
            Assert.Equal("Completed", GetString(fullBackup, "Status"));
        }
    }

    // #27: a deleted task's history is never removed, because only a successful full backup of the same task removes old rows
    [RavenFact(RavenTestCategory.BackupExportImport)]
    public async Task BackupHistoryCleaner_ShouldRemoveTheExpiredHistoryOfADeletedTask()
    {
        using var server = GetNewServer(new ServerCreationOptions { RunInMemory = true });
        using var store = GetDocumentStore(new Options { Server = server, RunInMemory = true });
        server.ServerStore.BackupHistoryCleaner.ForTestingPurposesOnly().RetentionPeriod = TimeSpan.FromHours(1);

        var config = Backup.CreateBackupConfiguration(NewDataPath(suffix: "BackupFolder"), fullBackupFrequency: "0 0 1 1 1");
        var result = await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config));
        await Cluster.WaitForRaftIndexToBeAppliedOnClusterNodesAsync(result.TaskId, nodes: [server]);

        var start = DateTime.UtcNow;
        await StoreHistoryEntryAsync(server, store.Database, result.TaskId, start, lastFullBackup: start);
        await StoreHistoryEntryAsync(server, store.Database, result.TaskId, start.AddMinutes(1), lastFullBackup: start, isFull: false);

        var deleteResult = await store.Maintenance.SendAsync(new DeleteOngoingTaskOperation(result.TaskId, OngoingTaskType.Backup));
        await Cluster.WaitForRaftIndexToBeAppliedOnClusterNodesAsync(deleteResult.RaftCommandIndex, nodes: [server]);

        Assert.Equal(0, await server.ServerStore.BackupHistoryCleaner.CleanupAsync(start));
        Assert.Equal(2, await server.ServerStore.BackupHistoryCleaner.CleanupAsync(start.AddHours(2)));
        Assert.Equal(0, CountStoredEntries(server));
        Assert.Equal(0, CountStoredDetails(server));
    }

    // #14: the history of a database deleted while this node did not delete it (down, or the delete failed) stays forever
    [RavenFact(RavenTestCategory.BackupExportImport)]
    public async Task BackupHistoryCleaner_ShouldRemoveTheHistoryOfADatabaseThatNoLongerExists()
    {
        using var server = GetNewServer(new ServerCreationOptions { RunInMemory = true });

        var start = DateTime.UtcNow;
        await StoreHistoryEntryAsync(server, "RavenDB-19358-no-such-database", taskId: 1, start, lastFullBackup: start);

        Assert.Equal(1, await server.ServerStore.BackupHistoryCleaner.CleanupAsync(start));
        Assert.Equal(0, CountStoredEntries(server));
        Assert.Equal(0, CountStoredDetails(server));
    }

    // #29: the history of a shard that was removed from the database stays forever
    [RavenFact(RavenTestCategory.BackupExportImport | RavenTestCategory.Sharding)]
    public async Task BackupHistoryCleaner_ShouldRemoveTheHistoryOfARemovedShard()
    {
        using var server = GetNewServer(new ServerCreationOptions { RunInMemory = true });
        using var store = Sharding.GetDocumentStore(new Options { Server = server });

        var start = DateTime.UtcNow;
        await StoreHistoryEntryAsync(server, ShardHelper.ToShardName(store.Database, 99), taskId: 1, start, lastFullBackup: start);

        Assert.Equal(1, await server.ServerStore.BackupHistoryCleaner.CleanupAsync(start));
        Assert.Equal(0, CountStoredEntries(server));
    }

    // #29: the retention period set on the database is not covered
    [RavenFact(RavenTestCategory.BackupExportImport)]
    public async Task BackupHistoryCleaner_ShouldUseTheRetentionPeriodOfTheDatabase()
    {
        using var server = GetNewServer(new ServerCreationOptions { RunInMemory = true });
        using var shortRetentionStore = GetDocumentStore(new Options
        {
            Server = server,
            RunInMemory = true,
            ModifyDatabaseRecord = record => record.Settings[RavenConfiguration.GetKey(x => x.Backup.BackupHistoryRetentionPeriod)] = "1"
        });
        using var defaultRetentionStore = GetDocumentStore(new Options { Server = server, RunInMemory = true });

        var start = DateTime.UtcNow;
        foreach (var store in new[] { shortRetentionStore, defaultRetentionStore })
            await StoreHistoryEntryAsync(server, store.Database, taskId: 0, start, lastFullBackup: start);

        Assert.Equal(1, await server.ServerStore.BackupHistoryCleaner.CleanupAsync(start.AddDays(2)));

        using (server.ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
        {
            Assert.Empty(await GetHistoryGroupsAsync(context, shortRetentionStore, $"database={shortRetentionStore.Database}"));
            Assert.Single(await GetHistoryGroupsAsync(context, defaultRetentionStore, $"database={defaultRetentionStore.Database}"));
        }
    }

    // #27: the cleanup deletes everything in one transaction
    [RavenFact(RavenTestCategory.BackupExportImport)]
    public async Task BackupHistoryCleaner_ShouldDeleteInBatches()
    {
        using var server = GetNewServer(new ServerCreationOptions { RunInMemory = true });
        using var store = GetDocumentStore(new Options { Server = server, RunInMemory = true });
        server.ServerStore.BackupHistoryCleaner.ForTestingPurposesOnly().MaxEntriesToDeletePerCommand = 2;

        var start = DateTime.UtcNow;
        for (var i = 0; i < 5; i++)
            await StoreHistoryEntryAsync(server, store.Database, taskId: 0, start.AddMinutes(i), lastFullBackup: start.AddMinutes(i));

        Assert.Equal(5, await server.ServerStore.BackupHistoryCleaner.CleanupAsync(start.AddDays(31)));
        Assert.Equal(0, CountStoredEntries(server));
    }

    // #28: a cleanup interval of 0 makes the cleaner run without a pause
    [RavenFact(RavenTestCategory.BackupExportImport)]
    public void BackupHistoryCleanupInterval_ShouldBeAtLeastOneMinute()
    {
        var configuration = RavenConfiguration.CreateForTesting("server", ResourceType.Server);
        configuration.SetSetting(RavenConfiguration.GetKey(x => x.Backup.BackupHistoryCleanupInterval), "0");
        configuration.Initialize();

        Assert.Equal(TimeSpan.FromMinutes(1), configuration.Backup.BackupHistoryCleanupInterval.AsTimeSpan);
    }

    // #31: a history retention of 0 days deletes every entry right away, except the newest full backup of each task
    [RavenFact(RavenTestCategory.BackupExportImport)]
    public async Task BackupHistoryCleaner_RetentionPeriodShouldBeAtLeastOneDay()
    {
        using var server = GetNewServer(new ServerCreationOptions { RunInMemory = true });
        using var store = GetDocumentStore(new Options
        {
            Server = server,
            RunInMemory = true,
            ModifyDatabaseRecord = record => record.Settings[RavenConfiguration.GetKey(x => x.Backup.BackupHistoryRetentionPeriod)] = "0"
        });

        var start = DateTime.UtcNow;
        await StoreHistoryEntryAsync(server, store.Database, taskId: 0, start, lastFullBackup: start);

        Assert.Equal(0, await server.ServerStore.BackupHistoryCleaner.CleanupAsync(start.AddHours(1)));
    }

    [RavenTheory(RavenTestCategory.BackupExportImport)]
    [InlineData("not-a-number")]
    [InlineData("999999")]
    public async Task BackupHistoryCleaner_InvalidRetentionPeriodOfOneDatabaseShouldNotStopTheCleanupOfTheOthers(string retentionPeriod)
    {
        using var server = GetNewServer(new ServerCreationOptions { RunInMemory = true });
        using var invalidRetentionStore = GetDocumentStore(new Options { Server = server, RunInMemory = true });
        using var defaultRetentionStore = GetDocumentStore(new Options { Server = server, RunInMemory = true });

        await invalidRetentionStore.Maintenance.SendAsync(new PutDatabaseSettingsOperation(invalidRetentionStore.Database, new Dictionary<string, string>
        {
            [RavenConfiguration.GetKey(x => x.Backup.BackupHistoryRetentionPeriod)] = retentionPeriod
        }));

        var start = DateTime.UtcNow;
        foreach (var store in new[] { invalidRetentionStore, defaultRetentionStore })
            await StoreHistoryEntryAsync(server, store.Database, taskId: 0, start, lastFullBackup: start);

        Assert.Equal(1, await server.ServerStore.BackupHistoryCleaner.CleanupAsync(start.AddDays(31)));

        using (server.ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            Assert.Empty(await GetHistoryGroupsAsync(context, defaultRetentionStore, $"database={defaultRetentionStore.Database}"));
    }

    // #18 (runner): a backup that fails to start shows the duration of the previous backup
    [RavenFact(RavenTestCategory.BackupExportImport)]
    public async Task BackupHistory_FailedStartShouldNotCarryThePreviousDuration()
    {
        var backupPath = NewDataPath(suffix: "BackupFolder");
        using var store = GetDocumentStore(new Options { RunInMemory = true });
        await StoreUserAsync(store, "users/1");

        var config = Backup.CreateBackupConfiguration(backupPath, fullBackupFrequency: "0 0 1 1 1");
        var result = await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config));
        await Cluster.WaitForRaftIndexToBeAppliedOnClusterNodesAsync(result.TaskId, nodes: [Server]);
        var successfulStatus = await Backup.RunBackupAndReturnStatusAsync(Server, result.TaskId, store, isFullBackup: true);
        Assert.True(successfulStatus.DurationInMs > 0);

        await FailBackupStartAsync(Server, store, result.TaskId, isFullBackup: true);

        using (Server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
        {
            var backupHistory = GetBackupHistoryFromEndpoint(context, store, new BackupHistoryRequestParameters { DatabaseName = store.Database });
            var failedStart = backupHistory.Groups.Single(group => group.FullBackup.Error != null).FullBackup;

            Assert.True(failedStart.DurationInMs is null or 0, $"The failed start shows {failedStart.DurationInMs} ms, taken from the previous backup");
        }
    }

    // Coverage: snapshot backups are recorded with their type
    [RavenFact(RavenTestCategory.BackupExportImport)]
    public async Task BackupHistory_ShouldStoreSnapshotBackups()
    {
        var backupPath = NewDataPath(suffix: "BackupFolder");
        using var store = GetDocumentStore(new Options { RunInMemory = false });
        await StoreUserAsync(store, "users/1");

        var config = Backup.CreateBackupConfiguration(backupPath, backupType: BackupType.Snapshot, fullBackupFrequency: "0 0 1 1 1");
        var result = await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config));
        await Cluster.WaitForRaftIndexToBeAppliedOnClusterNodesAsync(result.TaskId, nodes: [Server]);
        await Backup.RunBackupAsync(Server, result.TaskId, store, isFullBackup: true);
        await StoreUserAsync(store, "users/2");
        await Backup.RunBackupAsync(Server, result.TaskId, store, isFullBackup: false);

        using (Server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
        {
            var backupHistory = GetBackupHistoryFromEndpoint(context, store, new BackupHistoryRequestParameters { DatabaseName = store.Database });

            var group = Assert.Single(backupHistory.Groups);
            Assert.Equal(BackupType.Snapshot, group.FullBackup.BackupType);
            Assert.Null(group.FullBackup.Error);
            Assert.All(group.IncrementalBackups, entry => Assert.Equal(BackupType.Snapshot, entry.BackupType));
        }
    }

    private static BackupHistory CollectAndAssertBackupHistoryStructure(BackupHistory expectedBackupHistory, RavenServer server, DocumentStore documentStore, BackupHistoryRequestParameters parameters)
    {
        BackupHistory actualBackupHistory = null;

        using (server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
        using (context.OpenReadTransaction())
            WaitForValue(() =>
                {
                    actualBackupHistory = GetBackupHistoryFromEndpoint(context, documentStore, parameters);
                    return actualBackupHistory.Groups.Count == expectedBackupHistory.Groups.Count;
                },
                expectedVal: true,
                timeout: (int)TimeSpan.FromSeconds(15).TotalMilliseconds,
                interval: (int)TimeSpan.FromSeconds(1).TotalMilliseconds);

        Assert.NotNull(actualBackupHistory);
        actualBackupHistory.Groups.Sort((a, b) => a.FullBackup.CreatedAt.CompareTo(b.FullBackup.CreatedAt));

        Assert.True(actualBackupHistory.Groups.Count == expectedBackupHistory.Groups.Count,
            userMessage: $"Expected backup history groups count is `{expectedBackupHistory.Groups.Count}`, " +
                         $"but got `{actualBackupHistory.Groups.Count}`{Environment.NewLine}{actualBackupHistory.ToString(server.ServerStore.ContextPool)}");

        for (int i = 0; i < actualBackupHistory.Groups.Count; i++)
        {
            var actualBackupGroup = actualBackupHistory.Groups[i];
            var expectedBackupGroup = expectedBackupHistory.Groups[i];

            if (parameters.IncludeIncrementals)
            {
                Assert.True(actualBackupGroup.IncrementalBackups.Count == expectedBackupGroup.IncrementalBackups.Count,
                    userMessage: $"Expected size of incremental backups list on backup group with index `{i}` is `{expectedBackupGroup.IncrementalBackups.Count}`, " +
                                 $"but got `{actualBackupGroup.IncrementalBackups.Count}`{Environment.NewLine}{actualBackupHistory.ToString(server.ServerStore.ContextPool)}");
            }
            else
            {
                Assert.True(actualBackupGroup.IncrementalBackups.Count == 0,
                    userMessage: $"Expected size of incremental backups list on backup group with index `{i}` is `0`, " +
                                 $"but got `{actualBackupGroup.IncrementalBackups.Count}`{Environment.NewLine}{actualBackupHistory.ToString(server.ServerStore.ContextPool)}");
            }

            Assert.True(actualBackupGroup.IncrementalBackupsCount == expectedBackupGroup.IncrementalBackupsCount,
                userMessage: $"Expected incremental backups count on backup group with index `{i}` is `{expectedBackupGroup.IncrementalBackupsCount}`, " +
                             $"but got `{actualBackupGroup.IncrementalBackupsCount}`{Environment.NewLine}{actualBackupHistory.ToString(server.ServerStore.ContextPool)}");
        }

        return actualBackupHistory;
    }

    private static BackupHistory GetBackupHistoryFromEndpoint(TransactionOperationContext context, DocumentStore store, BackupHistoryRequestParameters parameters)
    {
        var client = store.GetRequestExecutor().HttpClient;

        var baseUrl = $"{store.Urls.First()}/periodic-backup/history";

        var builder = new UriBuilder(baseUrl);

        var query = HttpUtility.ParseQueryString(string.Empty);

        query["database"] = parameters.DatabaseName;

        if (parameters.TaskId != 0)
            query["taskId"] = parameters.TaskId.ToString();
        if (parameters.FullBackupId != 0)
            query["fullBackupCreatedAt"] = new DateTime(parameters.FullBackupId, DateTimeKind.Utc).ToString("O");
        if (parameters.IncludeIncrementals == false)
            query["includeIncrementals"] = false.ToString();

        builder.Query = query.ToString() ?? string.Empty;

        var fullUrl = builder.ToString();

        var response = AsyncHelpers.RunSync(() => client.GetAsync(fullUrl));
        string result = response.Content.ReadAsStringAsync().Result;

        var bjro = context.Sync.ReadForMemory(result, "BackupHistory");
        bjro.TryGet(nameof(BackupHistory), out BlittableJsonReaderObject backupHistoryBjro);

        return backupHistoryBjro == null ? null : JsonDeserializationServer.BackupHistory(backupHistoryBjro);
    }

    public class BackupHistoryRequestParameters
    {
        public string DatabaseName { get; set; }
        public long TaskId { get; set; }
        public bool IncludeIncrementals { get; set; } = true;
        public long FullBackupId { get; set; }
    }

    private static BackupResult GetBackupResultFromEndpoint(JsonOperationContext context, DocumentStore store, string databaseName, long taskId, DateTime createdAt)
    {
        var client = store.GetRequestExecutor().HttpClient;
        var response = AsyncHelpers.RunSync(() => client.GetAsync($"{store.Urls.First()}/periodic-backup/result?database={databaseName}&taskId={taskId}&createdAt={Uri.EscapeDataString(createdAt.ToString("O"))}"));
        string result = response.Content.ReadAsStringAsync().Result;

        var resultBjro = context.Sync.ReadForMemory(result, "Result");
        resultBjro.TryGet(nameof(BackupResult), out BlittableJsonReaderObject backupResultBjro);

        return backupResultBjro == null ? null : JsonDeserializationServer.BackupResult(backupResultBjro);
    }

    private async Task<BackupHistory> RunBackupsAccordingPlan(BackupKind[] backupPlan, RavenServer server, long taskId, DocumentStore store, BackupHistory existingBackupHistory = null)
    {
        BackupGroup backupGroup = null;

        if (existingBackupHistory != null)
            backupGroup = existingBackupHistory.Groups.LastOrDefault(x => x.TaskId == taskId);
        else
            existingBackupHistory = new BackupHistory(store.Database);

        foreach (var backupKind in backupPlan)
        {
            await Backup.RunBackupAsync(server, taskId, store, isFullBackup: backupKind.Equals(BackupKind.Full));

            switch (backupKind)
            {
                case BackupKind.Full:
                    backupGroup = new BackupGroup(taskId)
                    {
                        FullBackup = new BackupHistoryEntry { NodeTag = server.ServerStore.NodeTag, BackupKind = BackupKind.Full }
                    };

                    existingBackupHistory.Groups.Add(backupGroup);
                    break;

                case BackupKind.Incremental:
                    if (backupGroup == null)
                    {
                        backupGroup = new BackupGroup(taskId);
                        existingBackupHistory.Groups.Add(backupGroup);
                    }
                    backupGroup.AddIncrementalBackup(new BackupHistoryEntry { NodeTag = server.ServerStore.NodeTag, BackupKind = BackupKind.Incremental});
                    break;
            }
        }

        return existingBackupHistory;
    }

    private RavenServer GetServerThatUnloadsIdleDatabases()
    {
        return GetNewServer(new ServerCreationOptions
        {
            RunInMemory = false,
            CustomSettings = new Dictionary<string, string>
            {
                [RavenConfiguration.GetKey(x => x.Databases.MaxIdleTime)] = "10",
                [RavenConfiguration.GetKey(x => x.Databases.FrequencyToCheckForIdle)] = "3",
                [RavenConfiguration.GetKey(x => x.Backup.BackupRunnerFrequency)] = "1"
            }
        });
    }

    private async Task<(long TaskId, DateTime? LastFullBackup)> RunFullBackupAndWaitUntilIdleAsync(RavenServer server, DocumentStore store)
    {
        await StoreUserAsync(store, "users/1");

        var config = Backup.CreateBackupConfiguration(NewDataPath(suffix: "BackupFolder"), fullBackupFrequency: "0 0 1 1 *", incrementalBackupFrequency: "* * * * *");
        var taskId = await Backup.UpdateConfigAndRunBackupAsync(server, config, store);
        var lastFullBackup = server.ServerStore.DatabaseInfoCache.BackupStatusStorage.GetBackupStatus(store.Database, taskId).LastFullBackup;

        server.ServerStore.DatabasesLandlord.ForTestingPurposesOnly().SkipIncreasingLastWorkTimeBasedOnDatabaseSize = true;
        Assert.True(WaitForValue(() => server.ServerStore.IdleDatabases.ContainsKey(store.Database), true, timeout: 90_000, interval: 500),
            "precondition: the database should become idle");

        return (taskId, lastFullBackup);
    }

    private static StrongBox<int> CountDatabaseLoads(RavenServer server, string databaseName)
    {
        var loads = new StrongBox<int>();
        server.ServerStore.DatabasesLandlord.ForTestingPurposesOnly().AfterDatabaseCreation = args =>
        {
            if (string.Equals(args.Database?.Name, databaseName, StringComparison.OrdinalIgnoreCase))
                Interlocked.Increment(ref loads.Value);
        };

        return loads;
    }

    private static async Task<BackupHistoryEntry> WaitForIncrementalEntryAsync(RavenServer server, DocumentStore store, long taskId, DateTime since)
    {
        BackupHistoryEntry entry = null;

        await WaitForValueAsync(() =>
        {
            using (server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                var backupHistory = GetBackupHistoryFromEndpoint(context, store, new BackupHistoryRequestParameters { DatabaseName = store.Database, TaskId = taskId });
                entry = backupHistory?.Groups.SelectMany(group => group.IncrementalBackups).FirstOrDefault(incremental => incremental.CreatedAt >= since);
                return Task.FromResult(entry != null);
            }
        }, true, timeout: 150_000, interval: 1000);

        return entry;
    }

    private static async Task FailBackupStartAsync(RavenServer server, DocumentStore store, long taskId, bool isFullBackup)
    {
        var database = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
        server.ServerStore.ServerBackupRunner.ForTestingPurposesOnly().DatabaseTestingStuffInternals[database.Name] =
            new ServerBackupRunner.TestingStuffInternal { SimulateFailedBackupStart = true };

        Assert.Throws<InvalidOperationException>(() => server.ServerStore.ServerBackupRunner.StartBackupNowTask(database, taskId, isFullBackup, database.Operations.GetNextOperationId()));
    }

    private static async Task RunOneTimeBackupAsync(DocumentStore store, string backupPath)
    {
        var operation = await store.Maintenance.SendAsync(new BackupOperation(new BackupConfiguration
        {
            BackupType = BackupType.Backup,
            LocalSettings = new LocalSettings { FolderPath = backupPath }
        }));
        await operation.WaitForCompletionAsync(TimeSpan.FromSeconds(30));
    }

    private static async Task StoreUserAsync(DocumentStore store, string id)
    {
        using (var session = store.OpenAsyncSession())
        {
            await session.StoreAsync(new User { Name = "RavenDB-19358" }, id);
            await session.SaveChangesAsync();
        }
    }

    private static Task StoreHistoryEntryAsync(RavenServer server, string databaseName, long taskId, DateTime createdAt, DateTime? lastFullBackup, bool isFull = true, string error = null,
        string taskName = "RavenDB-19358", long? durationInMs = null)
    {
        var status = new PeriodicBackupStatus
        {
            TaskId = taskId,
            IsFull = isFull,
            BackupType = BackupType.Backup,
            NodeTag = server.ServerStore.NodeTag,
            DurationInMs = durationInMs,
            LastFullBackup = lastFullBackup,
            LastFullBackupInternal = isFull ? createdAt : lastFullBackup,
            LastIncrementalBackupInternal = isFull ? null : createdAt,
            Error = error == null ? null : new Error { Exception = error, At = createdAt }
        };

        var entry = new BackupHistoryEntry(status, taskName, BackupTrigger.Scheduled, skipped: false);
        return server.ServerStore.Engine.TxMerger.Enqueue(new UpdateBackupHistoryCommand([new BackupHistoryUpdate(databaseName, taskId, entry, new BackupResult())]));
    }

    private static long CountStoredEntries(RavenServer server)
    {
        using (server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            return server.ServerStore.DatabaseInfoCache.BackupHistoryStorage.ForTestingPurposesOnly().BackupHistoryEntriesNumber(context);
    }

    private static long CountStoredDetails(RavenServer server)
    {
        using (server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            return server.ServerStore.DatabaseInfoCache.BackupHistoryStorage.ForTestingPurposesOnly().BackupResultDetailsNumber(context);
    }

    private static Task<HttpResponseMessage> GetFromEndpointAsync(DocumentStore store, string endpoint, string query)
    {
        return store.GetRequestExecutor().HttpClient.GetAsync($"{store.Urls.First()}/periodic-backup/{endpoint}?{query}");
    }

    private static async Task<BlittableJsonReaderObject> GetHistoryJsonAsync(JsonOperationContext context, DocumentStore store, string query)
    {
        using var response = await GetFromEndpointAsync(store, "history", query);
        var body = await response.Content.ReadAsStringAsync();
        Assert.True(response.IsSuccessStatusCode, $"GET history?{query} returned {(int)response.StatusCode}: {body}");

        var json = context.Sync.ReadForMemory(body, "backup-history");
        return GetObject(json, nameof(BackupHistory));
    }

    private static async Task<List<BlittableJsonReaderObject>> GetHistoryGroupsAsync(JsonOperationContext context, DocumentStore store, string query)
    {
        return GetObjects(await GetHistoryJsonAsync(context, store, query), "Groups");
    }

    private static BlittableJsonReaderObject GetObject(BlittableJsonReaderObject json, string name)
    {
        Assert.True(json.TryGet(name, out BlittableJsonReaderObject value) && value != null, $"Missing `{name}` in {json}");
        return value;
    }

    private static List<BlittableJsonReaderObject> GetObjects(BlittableJsonReaderObject json, string name)
    {
        return json.TryGet(name, out BlittableJsonReaderArray array) && array != null
            ? array.Cast<BlittableJsonReaderObject>().ToList()
            : [];
    }

    private static string GetString(BlittableJsonReaderObject json, string name)
    {
        return json.TryGet(name, out string value) ? value : null;
    }

    private static DateTime GetDateTime(BlittableJsonReaderObject json, string name)
    {
        return DateTime.Parse(GetString(json, name), CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind);
    }

    private static long GetLong(BlittableJsonReaderObject json, string name)
    {
        Assert.True(json.TryGet(name, out long value), $"Missing `{name}` in {json}");
        return value;
    }

    private static bool? GetBool(BlittableJsonReaderObject json, string name)
    {
        return json.TryGet(name, out bool value) ? value : null;
    }
}
