using System.Collections.Generic;
using System.Threading.Tasks;
using System;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.CompareExchange;
using Raven.Client.Documents.Session;
using Raven.Server.Config;
using Raven.Server.ServerWide.Context;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Json;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_20150 : ClusterTestBase
{
    public RavenDB_20150(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.CompareExchange | RavenTestCategory.Cluster)]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    public async Task CompareExchangeTombstoneWillBeCleanedOnlyWhenAllNodesHaveBackedUpPreviousOnes(int replicationFactor)
    {
        var database = GetDatabaseName();
        var settings = new Dictionary<string, string>
            {
                { RavenConfiguration.GetKey(x => x.Cluster.CompareExchangeTombstonesCleanupInterval), "0" },
            };
        var (nodes, leader) = await CreateRaftCluster(3, watcherCluster: true, leaderIndex: 0, customSettings: settings);
        var (_, databaseServers) = await CreateDatabaseInCluster(database, replicationFactor, leader.WebUrl);

        var backupPath = NewDataPath(suffix: $"BackupFolderNonSharded");

        using (var store = new DocumentStore()
        {
            Database = database,
            Urls = new[] { databaseServers[Math.Max(0, databaseServers.Count - 2)].WebUrl }
        }.Initialize())
        {
            var user = new User
            {
                Name = "🤡"
            };

            //create one compare exchange on each shard
            var cxRes = await store.Operations.SendAsync(new PutCompareExchangeValueOperation<User>("users/1", user, 0));

            //suspend observer to stall tombstone cleaning
            leader.ServerStore.Observer.Suspended = true;

            var timeBeforeCxDeletion = DateTime.UtcNow;

            //delete the compare exchanges
            await store.Operations.SendAsync(new DeleteCompareExchangeValueOperation<User>("users/1", cxRes.Index));

            //run periodic backup on other database server
            var config = Backup.CreateBackupConfiguration(backupPath, incrementalBackupFrequency: "0 0 1 * *");
            var result = await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config));

            foreach (var dbServer in databaseServers)
            {
                var server2Database = await dbServer.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
                var periodicBackupRunner = server2Database.PeriodicBackupRunner;
                var op = periodicBackupRunner.StartBackupTask(result.TaskId, isFullBackup: false);
                var value = await WaitForValueAsync(() =>
                {
                    var status = server2Database.Operations.GetOperation(op)?.State.Status;
                    return status;
                }, OperationStatus.Completed);
                Assert.Equal(OperationStatus.Completed, value);

                //wait for periodic backup to finish running
                var done = await WaitForValueAsync(() =>
                {
                    using (dbServer.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                    using (context.OpenReadTransaction())
                    {
                        var itemName = PeriodicBackupStatus.GenerateItemName(database, result.TaskId);
                        var status = dbServer.ServerStore.Cluster.Read(context, itemName);
                        if (status == null)
                            return false;
                        status.TryGet(nameof(LastRaftIndex), out BlittableJsonReaderObject lastRaftIndexBlittable);
                        lastRaftIndexBlittable.TryGet(nameof(LastRaftIndex.LastEtag), out long etag);
                        return etag >= cxRes.Index;
                    }
                }, true);

                Assert.True(done);

                //Running cluster transaction to immediately mark the tombstone as handled by the cluster transaction mechanism and allow cleaning it
                using (var session = store.OpenAsyncSession(new SessionOptions{TransactionMode = TransactionMode.ClusterWide, DisableAtomicDocumentWritesInClusterWideTransaction = true}))
                {
                    await session.StoreAsync(new TestOjb());
                    await session.SaveChangesAsync();
                }
            }

            //unsuspend and wait for the tombstone cleaner
            leader.ServerStore.Observer.Suspended = false;

            await WaitAndAssertForGreaterThanAsync(() => Task.FromResult(leader.ServerStore.Observer._lastTombstonesCleanupTimeInTicks), timeBeforeCxDeletion.Ticks);

            //ensure compare exchange tombstones were deleted after the tombstone cleanup
            foreach (var node in nodes)
            {
                long numOfCompareExchangeTombstones = -1;
                long numOfCompareExchanges = -1;
                await WaitForValueAsync(() =>
                {
                    using (node.ServerStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
                    using (context.OpenReadTransaction())
                    {
                        numOfCompareExchangeTombstones = node.ServerStore.Cluster.GetNumberOfCompareExchangeTombstones(context, store.Database);
                        numOfCompareExchanges = node.ServerStore.Cluster.GetNumberOfCompareExchange(context, store.Database);

                        return numOfCompareExchanges == 0 && numOfCompareExchangeTombstones == 0;
                    }
                }, true);

                Assert.Equal(0, numOfCompareExchangeTombstones);
                Assert.Equal(0, numOfCompareExchanges);
            }
        }
    }
    
    private class TestOjb
    {
        public string Id { get; set; }
    }
}
