using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.CompareExchange;
using Raven.Client.Http;
using Raven.Client.ServerWide.Commands;
using Raven.Server.Config;
using Raven.Server.Extensions;
using Sparrow.Json;
using Tests.Infrastructure;
using Xunit;

namespace InterversionTests;

public class ClusterMaintenanceSupervisorInterversionTests : MixedClusterTestBase
{
    public ClusterMaintenanceSupervisorInterversionTests(ITestOutputHelper output) : base(output)
    {
    }

    [RavenMultiplatformFact(RavenTestCategory.BackupExportImport | RavenTestCategory.CompareExchange | RavenTestCategory.Interversion, RavenPlatform.Windows)]
    public async Task CompareExchangeTombstoneCleaner_ShouldNotDeleteTombstones_IfOldVersionServerInCluster()
    {
        // Setting up the testing environment with two nodes running version 5.4.101 and the current codebase version
        var backupPath = NewDataPath(suffix: "BackupFolder");
        var databaseName = GetDatabaseName();
        const int compareExchangeTombstonesCleanupInterval = 1;

        var customSettings = new Dictionary<string, string> { { RavenConfiguration.GetKey(x => x.Cluster.CompareExchangeTombstonesCleanupInterval), compareExchangeTombstonesCleanupInterval.ToString() } };
        var process628List = await CreateCluster(["6.2.8", "6.2.8"], customSettings: customSettings);

        var interversionTestOptions = new InterversionTestOptions { ReplicationFactor = 2, CreateDatabase = true };
        var currentVersionProcessNode = process628List[0];
        var version628ProcessNode = process628List[1];
        await UpgradeServerAsync(toVersion: "current", currentVersionProcessNode, customSettings);

        using var currentVersionStore = await GetStore(currentVersionProcessNode.Url, currentVersionProcessNode.Process, databaseName, interversionTestOptions);

        interversionTestOptions.CreateDatabase = false;
        using var store628 = await GetStore(version628ProcessNode.Url, version628ProcessNode.Process, databaseName, interversionTestOptions);
        long backupTaskId;

        // Ensuring that the current version server is the leader of the cluster
        using (var requestExecutor = ClusterRequestExecutor.CreateForShortTermUse(currentVersionStore.Urls[0], certificate: null, DocumentConventions.DefaultForServer))
        using (requestExecutor.ContextPool.AllocateOperationContext(out JsonOperationContext context))
        {
            var getClusterTopologyCommand = new GetClusterTopologyCommand();
            await requestExecutor.ExecuteAsync(getClusterTopologyCommand, context);

            var clusterTopology = getClusterTopologyCommand.Result;

            var leaderTag = clusterTopology.Leader;
            if (clusterTopology.Topology.AllNodes.TryGetValue(leaderTag, out var leaderUrl) == false)
                throw new InvalidOperationException($"Leader node tag '{leaderTag}' not found in the cluster topology.");

            if (currentVersionProcessNode.Url != leaderUrl)
            {
                using var client = new HttpClient().WithConventions(store628.Conventions);
                await WaitForValueAsync(async () =>
                    {
                        await client.PostAsync($"{version628ProcessNode.Url}/admin/cluster/reelect", null);

                        getClusterTopologyCommand.Result = null;

                        await WaitForNotNullAsync(async () =>
                            {
                                await requestExecutor.ExecuteAsync(getClusterTopologyCommand, context);

                                leaderTag = getClusterTopologyCommand.Result?.Leader;
                                return leaderTag;
                            },
                            timeout:(int)TimeSpan.FromSeconds(10).TotalMilliseconds,
                            interval:(int)TimeSpan.FromMilliseconds(500).TotalMilliseconds);

                        if (clusterTopology.Topology.AllNodes.TryGetValue(leaderTag, out leaderUrl) == false)
                            throw new InvalidOperationException($"Leader node tag '{leaderTag}' not found in the cluster topology.");

                        return currentVersionProcessNode.Url == leaderUrl;
                    },
                    expectedVal: true,
                    timeout: (int)TimeSpan.FromSeconds(15).TotalMilliseconds,
                    interval: (int)TimeSpan.FromMilliseconds(500).TotalMilliseconds);
            }

            Assert.True(currentVersionProcessNode.Url == leaderUrl, "Expected the current version server to be the leader of the cluster, but it was not.");
            var version628NodeTag = clusterTopology.Topology.AllNodes.Single(kvp => kvp.Value == version628ProcessNode.Url).Key;

            // Creating compare exchange values and tombstones
            await currentVersionStore.Operations.SendAsync(new PutCompareExchangeValueOperation<int>("cx/1", 1, 0));
            await currentVersionStore.Operations.SendAsync(new PutCompareExchangeValueOperation<int>("cx/2", 1, 0));
            await CreateCompareExchangeTombstone(currentVersionStore, "cx/3");

            await AssertCompareExchangeCounts(currentVersionStore, expectedTombstonesNumber: 1, expectedCompareExchangeNumber: 2);
            await AssertCompareExchangeCounts(store628, expectedTombstonesNumber: 1, expectedCompareExchangeNumber: 2);

            var backupConfiguration = Backup.CreateBackupConfiguration(backupPath, mentorNode: version628NodeTag);
            var updatePeriodicBackupOperationResult = await currentVersionStore.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(backupConfiguration));
            backupTaskId = updatePeriodicBackupOperationResult.TaskId;
        }

        Operation<StartBackupOperationResult> operation = null;
        await WaitForValueAsync(async () =>
            {
                try
                {
                    operation = await store628.Maintenance.SendAsync(new StartBackupOperation(isFullBackup: true, backupTaskId));
                    return true;
                }
                catch
                {
                    return false;
                }
            },
            expectedVal: true,
            timeout: (int) TimeSpan.FromMinutes(1).TotalMilliseconds,
            interval: (int) TimeSpan.FromSeconds(1).TotalMilliseconds);

        await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

        // Let's wait double the cleanup interval to ensure that the tombstone cleaner has run at least once to avoid false positives.
        await Task.Delay(TimeSpan.FromMinutes(compareExchangeTombstonesCleanupInterval * 2));

        // Old versions of servers do not use the current mechanism for providing information about their backups, and we expect that all tombstones will remain undeleted
        await AssertCompareExchangeCounts(currentVersionStore, expectedTombstonesNumber: 1, expectedCompareExchangeNumber: 2);
        await AssertCompareExchangeCounts(store628, expectedTombstonesNumber: 1, expectedCompareExchangeNumber: 2);
    }

    private static async Task AssertCompareExchangeCounts(DocumentStore store, long expectedTombstonesNumber, long expectedCompareExchangeNumber, int timeout = 5000, int interval = 500)
    {
        var actualTombstonesNumber = await WaitForValueAsync(async () =>
        {
            var databaseStatistics = await store.Maintenance.ForDatabase(store.Database).SendAsync(new GetDetailedStatisticsOperation());
            return databaseStatistics.CountOfCompareExchangeTombstones;
        }, expectedVal: expectedTombstonesNumber, timeout, interval);

        var actualCompareExchangeNumber = await WaitForValueAsync( async () =>
        {
            var databaseStatistics = await store.Maintenance.ForDatabase(store.Database).SendAsync(new GetDetailedStatisticsOperation());
            return databaseStatistics.CountOfCompareExchange;
        }, expectedCompareExchangeNumber, timeout, interval);

        Assert.True(expectedTombstonesNumber == actualTombstonesNumber, $"Tombstones check failed. Expected: {expectedTombstonesNumber}, Actual: {actualTombstonesNumber}.");
        Assert.True(expectedCompareExchangeNumber == actualCompareExchangeNumber, $"Values check failed. Expected: {expectedCompareExchangeNumber}, Actual: {actualCompareExchangeNumber}.");
    }
    
    internal static async Task CreateCompareExchangeTombstone(DocumentStore documentStore, string key)
    {
        var res = await documentStore.Operations.SendAsync(new PutCompareExchangeValueOperation<int>(key, 1, 0));
        await documentStore.Operations.SendAsync(new DeleteCompareExchangeValueOperation<int>(key, res.Index));
    }
}
