using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Server;
using Raven.Server.Config;
using Raven.Server.Extensions;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Server.Json.Sync;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_20084 : ClusterTestBase
{
    public RavenDB_20084(ITestOutputHelper output) : base(output)
    {
    }

    [Fact]
    public async Task ShouldUpdateBackupInfoIfDatabaseUnloaded()
    {
        const int clusterSize = 3;
        const int backupIntervalInMinutes = 1;

        var backupPath = NewDataPath(suffix: "BackupFolder");
        var databaseName = GetDatabaseName();

        var (_, leaderServer) = await CreateRaftCluster(numberOfNodes: clusterSize, shouldRunInMemory: false,
            customSettings: new Dictionary<string, string>()
            {
                [RavenConfiguration.GetKey(x => x.Databases.MaxIdleTime)] = "1",
                [RavenConfiguration.GetKey(x => x.Databases.FrequencyToCheckForIdle)] = "1",
            });

        await CreateDatabaseInCluster(databaseName, clusterSize, leaderServer.WebUrl);

        using (var leaderStore = new DocumentStore())
        {
            leaderStore.Urls = new[] { leaderServer.WebUrl };
            leaderStore.Conventions = new DocumentConventions { DisableTopologyUpdates = true };
            leaderStore.Database = databaseName;
            leaderStore.Initialize();
            var debugInfo = new List<string> { $"Started at: {DateTime.UtcNow:yyyy-MM-ddTHH:mm:ss.ffffffZ}" };

            leaderServer.ServerStore.DatabasesLandlord.ForTestingPurposesOnly().OnFailedRescheduleNextScheduledActivity = (exception, erroredDatabaseName) =>
                debugInfo.Add($"Failed to schedule the next activity for the idle database '{erroredDatabaseName}': {exception}");

            // Populating the database and forcibly transitioning it to an idle state
            await Backup.FillClusterDatabaseWithRandomDataAsync(databaseSizeInMb: 1, leaderStore, clusterSize);

            var config = Backup.CreateBackupConfiguration(backupPath, fullBackupFrequency: "0 0 1 1 1",
                incrementalBackupFrequency: $"*/{backupIntervalInMinutes} * * * *", mentorNode: leaderServer.ServerStore.NodeTag);
            var taskId = await Backup.UpdateConfigAndRunBackupAsync(leaderServer, config, leaderStore);

            var onGoingTaskBackup = await leaderStore.Maintenance.SendAsync(new GetOngoingTaskInfoOperation(taskId, OngoingTaskType.Backup)) as OngoingTaskBackup;
            Assert.True(onGoingTaskBackup is { LastFullBackup: not null });
            var expectedTime = onGoingTaskBackup.NextBackup.DateTime;

            leaderServer.ServerStore.DatabasesLandlord.ForTestingPurposesOnly().ShouldFetchIdleStateImmediately = true;
            leaderServer.ServerStore.DatabasesLandlord.SkipShouldContinueDisposeCheck = true;

            using (leaderServer.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext serverStoreContext))
            using (serverStoreContext.OpenReadTransaction())
            {
                // Ensuring the database is in an idle state
                WaitForValue(() => leaderServer.ServerStore.IdleDatabases.Count,
                    expectedVal: 1,
                    timeout: Convert.ToInt32(TimeSpan.FromMinutes(5).TotalMilliseconds),
                    interval: Convert.ToInt32(TimeSpan.FromSeconds(1).TotalMilliseconds));
                Assert.Equal(1, leaderServer.ServerStore.IdleDatabases.Count);

                // No longer forcing the idle state for the database
                leaderServer.ServerStore.DatabasesLandlord.ForTestingPurposesOnly().ShouldFetchIdleStateImmediately = false;
                leaderServer.ServerStore.DatabasesLandlord.SkipShouldContinueDisposeCheck = false;

                // Awaiting the next backup event to verify that the '/database' endpoint provides an updated value for the last incremental backup timestamp
                using var client = new HttpClient().WithConventions(leaderStore.Conventions);
                DateTime lastBackupTime = default;
                await WaitForValueAsync(async () =>
                    {
                        try
                        {
                            var response = await client.GetAsync($"{leaderServer.WebUrl}/databases");
                            string result = await response.Content.ReadAsStringAsync();

                            var databaseResponse = serverStoreContext.Sync.ReadForMemory(result, "Databases");

                            if (databaseResponse.TryGet("Databases", out BlittableJsonReaderArray array) == false)
                            {
                                debugInfo.Add("Unable to get Databases array from context");
                                debugInfo.Add(result);
                                return false;
                            }

                            debugInfo.Add($"Successfully extracted Databases array from context, array.Length = '{array.Length}'");

                            if (((BlittableJsonReaderObject)array[0]).TryGet("BackupInfo", out BlittableJsonReaderObject backupInfo) == false)
                            {
                                debugInfo.Add("Unable to get BackupInfo from Database");
                                debugInfo.Add(array.ToString());
                                return false;
                            }

                            if (backupInfo == null)
                            {
                                debugInfo.Add("Unable to get BackupInfo from Database");
                                debugInfo.Add(array.ToString());
                                return false;
                            }

                            debugInfo.Add("Successfully extracted BackupInfo from Database");

                            if (backupInfo.TryGet("LastBackup", out lastBackupTime) == false)
                            {
                                debugInfo.Add("Unable to get LastBackup from BackupInfo");
                                debugInfo.Add(backupInfo.ToString());
                                return false;
                            }

                            debugInfo.Add($"Successfully extracted LastBackup from Database: {lastBackupTime:yyyy-MM-ddTHH:mm:ss.ffffffZ}");
                        }
                        catch (Exception e)
                        {
                            debugInfo.Add(e.Message);
                        }

                        return lastBackupTime >= expectedTime;
                    },
                    expectedVal: true,
                    timeout: Convert.ToInt32(TimeSpan.FromMinutes(3).TotalMilliseconds),
                    interval: Convert.ToInt32(TimeSpan.FromSeconds(1).TotalMilliseconds));

                bool backupInfoUpdated = await AddInfo(debugInfo, lastBackupTime, expectedTime, leaderStore, taskId, client, leaderServer);

                Assert.True(backupInfoUpdated, $"lastBackupTime >= expectedTime: false{Environment.NewLine}{string.Join(Environment.NewLine, debugInfo)}");

                // Verifying that the cluster storage contains the same value for consistency
                var operation = new GetPeriodicBackupStatusOperation(taskId);
                var backupStatus = (await leaderStore.Maintenance.SendAsync(operation)).Status;

                var lastBackupTimeInClusterStorage = backupStatus.LastIncrementalBackup > backupStatus.LastFullBackup
                    ? backupStatus.LastIncrementalBackup
                    : backupStatus.LastFullBackup;

                var lastInternalBackupTimeInClusterStorage = backupStatus.LastIncrementalBackupInternal > backupStatus.LastFullBackupInternal
                    ? backupStatus.LastIncrementalBackupInternal
                    : backupStatus.LastFullBackupInternal;

                Assert.True(lastBackupTimeInClusterStorage >= expectedTime,
                    $"lastBackupTimeInClusterStorage >= expectedTime: false{Environment.NewLine}{string.Join(Environment.NewLine, debugInfo)}");
                Assert.True(lastInternalBackupTimeInClusterStorage >= expectedTime,
                    $"lastInternalBackupTimeInClusterStorage >= expectedTime: false{Environment.NewLine}{string.Join(Environment.NewLine, debugInfo)}");
            }
        }
    }

    private static async Task<bool> AddInfo(List<string> debugInfo, DateTime lastBackupTime, DateTime expectedTime, DocumentStore leaderStore, long taskId, HttpClient client,
        RavenServer leaderServer)
    {
        OngoingTaskBackup onGoingTaskBackup;
        debugInfo.Add($"'lastBackupTime': {lastBackupTime:yyyy-MM-ddTHH:mm:ss.ffffffZ}");
        debugInfo.Add($"'expectedTime':   {expectedTime:yyyy-MM-ddTHH:mm:ss.ffffffZ}");
        debugInfo.Add($"'Now':   {DateTime.UtcNow:yyyy-MM-ddTHH:mm:ss.ffffffZ}");
        onGoingTaskBackup = await leaderStore.Maintenance.SendAsync(new GetOngoingTaskInfoOperation(taskId, OngoingTaskType.Backup)) as OngoingTaskBackup;
        debugInfo.Add($"'LastIncrementalBackup':   {onGoingTaskBackup?.LastIncrementalBackup:yyyy-MM-ddTHH:mm:ss.ffffffZ}");
        debugInfo.Add($"'Error':   {onGoingTaskBackup?.Error}");
        debugInfo.Add($"'TaskConnectionStatus':   {onGoingTaskBackup?.TaskConnectionStatus}");
        debugInfo.Add($"'LastFullBackup':   {onGoingTaskBackup?.LastFullBackup}");
        debugInfo.Add($"'TaskState':   {onGoingTaskBackup?.TaskState}");
        var backupInfoUpdated = lastBackupTime >= expectedTime;
        if (backupInfoUpdated == false)
        {
            var response = await client.GetAsync($"{leaderServer.WebUrl}/admin/debug/databases/idle");
            debugInfo.Add($"{leaderServer.WebUrl}/admin/debug/databases/idle");
            debugInfo.Add(await response.Content.ReadAsStringAsync());

            response = await client.GetAsync($"{leaderServer.WebUrl}/admin/debug/periodic-backup/timers");
            debugInfo.Add($"{leaderServer.WebUrl}/admin/debug/periodic-backup/timers");
            debugInfo.Add(await response.Content.ReadAsStringAsync());
        }

        return backupInfoUpdated;
    }
}
