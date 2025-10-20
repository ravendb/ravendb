using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;
using System.Web;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Util;
using Raven.Server;
using Raven.Server.Documents;
using Raven.Server.Documents.PeriodicBackup;
using Raven.Server.Documents.PeriodicBackup.BackupHistory;
using Raven.Server.Json;
using Raven.Server.ServerWide.Context;
using SlowTests.Core.Utils.Entities;
using Sparrow.Json;
using Sparrow.Server.Json.Sync;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

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
        documentDatabase.PeriodicBackupRunner.ForTestingPurposesOnly().SimulateFailedBackup = true;

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
        var expectedError = new Exception(nameof(DocumentDatabase.PeriodicBackupRunner._forTestingPurposes.SimulateFailedBackup)).ToString();
        Assert.True(fullBackup.Error.Contains(expectedError));

        // FullBackupResult existence assertion
        Assert.NotNull(backupResult);
        Assert.Equal(0, backupResult.Documents.ReadCount);
        Assert.True(backupResult.Elapsed > TimeSpan.Zero);
    }

    [RavenFact(RavenTestCategory.BackupExportImport)]
    public async Task BackupHistory_ShouldRespectRetentionPolicy()
    {
        var retentionPeriod = TimeSpan.FromSeconds(20);
        var backupPath = NewDataPath(suffix: "BackupFolder");
        var server = GetNewServer(new ServerCreationOptions { RunInMemory = true });

        // Configure the custom retention period for testing purposes
        server.ServerStore.DatabaseInfoCache.BackupHistoryStorage.ForTestingPurposesOnly().CustomBackupHistoryRetentionConfiguration = retentionPeriod;

        var backupCounter = 0;
        foreach (var _ in Enumerable.Range(0,2))
        {
            using var store = GetDocumentStore(new Options { Server = server });

            // Create two backup configurations with different taskIds
            var taskIds = new List<long>();
            foreach (var j in Enumerable.Range(0,2))
            {
                var config = Backup.CreateBackupConfiguration(backupPath, fullBackupFrequency: "0 0 1 1 1", name: $"BackupTask_{store.Database}_{j}");
                var result = store.Maintenance.Send(new UpdatePeriodicBackupOperation(config));
                await Cluster.WaitForRaftIndexToBeAppliedOnClusterNodesAsync(result.TaskId, nodes: [server]);
                taskIds.Add(result.TaskId);
            }

            // First series of backups
            var expectedBackupHistory = new BackupHistory(store.Database);
            var initialBackupsStartTime = DateTime.UtcNow;
            foreach (var taskId in taskIds)
                await RunBackupsAccordingPlan([BackupKind.Full, BackupKind.Incremental], server, taskId, store, expectedBackupHistory);

            await Task.Delay(retentionPeriod / 2);

            // Second series of backups
            foreach (var taskId in taskIds)
                await RunBackupsAccordingPlan([BackupKind.Full, BackupKind.Incremental, BackupKind.Incremental], server, taskId, store, expectedBackupHistory);

            // Wait until the retention period is elapsed for the first series
            await Task.Delay(retentionPeriod / 2);

            // Perform another full backup to trigger the retention policy
            foreach (var taskId in taskIds)
                await RunBackupsAccordingPlan([BackupKind.Full, BackupKind.Incremental, BackupKind.Incremental, BackupKind.Incremental], server, taskId, store, expectedBackupHistory);

            // Expecting that the oldest backups will be removed for each task
            expectedBackupHistory.Groups.RemoveRange(index: 0, count: taskIds.Count);

            // Verify backup history state after retention policy applied
            var parameters = new BackupHistoryRequestParameters{ DatabaseName = store.Database };
            var actualBackupHistory = CollectAndAssertBackupHistoryStructure(expectedBackupHistory, server, store, parameters);

            Assert.True(actualBackupHistory.DatabaseName == expectedBackupHistory.DatabaseName,
                userMessage: $"Expected database name is `{expectedBackupHistory.DatabaseName}`, but got `{actualBackupHistory.DatabaseName}`{Environment.NewLine}{actualBackupHistory.ToString(server.ServerStore.ContextPool)}");

            Assert.True(actualBackupHistory.Groups[0].FullBackup.CreatedAt < initialBackupsStartTime + retentionPeriod,
                userMessage: $"Expected oldest full backup date is less than `{DateTime.UtcNow + retentionPeriod}`, but got `{actualBackupHistory.Groups[0].FullBackup}`{Environment.NewLine}{actualBackupHistory.ToString(server.ServerStore.ContextPool)}");

            foreach (long taskId in taskIds)
            {
                var actualNumberOfGroupsPerTaskId = actualBackupHistory.Groups.Count(x => x.TaskId == taskId);
                var expectedNumberOfGroupsPerTaskId = expectedBackupHistory.Groups.Count(x => x.TaskId == taskId);

                Assert.True(actualNumberOfGroupsPerTaskId == expectedNumberOfGroupsPerTaskId,
                    userMessage: $"Expected number of groups for task id `{taskId}` is `{expectedNumberOfGroupsPerTaskId}`, but got `{actualNumberOfGroupsPerTaskId}`{Environment.NewLine}{actualBackupHistory.ToString(server.ServerStore.ContextPool)}");
            }

            backupCounter += expectedBackupHistory.Groups.Count + expectedBackupHistory.Groups.Sum(group => group.IncrementalBackups.Count);
        }

        using (server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
        {
            var backupHistoryEntriesNumber = server.ServerStore.DatabaseInfoCache.BackupHistoryStorage.ForTestingPurposesOnly().BackupHistoryEntriesNumber(context);
            var backupResultDetailsNumber = server.ServerStore.DatabaseInfoCache.BackupHistoryStorage.ForTestingPurposesOnly().BackupResultDetailsNumber(context);

            Assert.Equal(backupCounter, backupHistoryEntriesNumber);
            Assert.Equal(backupCounter, backupResultDetailsNumber);
        }
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
            query["fullBackupTicks"] = parameters.FullBackupId.ToString();
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
        var response = AsyncHelpers.RunSync(() => client.GetAsync($"{store.Urls.First()}/periodic-backup/result?database={databaseName}&taskId={taskId}&backupTicks={createdAt.Ticks}"));
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
}
