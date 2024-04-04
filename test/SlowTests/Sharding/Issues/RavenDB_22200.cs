using System;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Smuggler;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Documents.Sharding.Commands;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using SlowTests.Issues;
using Sparrow.Json.Parsing;
using Sparrow.Server.Collections;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Sharding.Issues
{
    public class RavenDB_22200 : ReplicationTestBase
    {
        public RavenDB_22200(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Sharding | RavenTestCategory.BackupExportImport)]
        public async Task ShouldDelayMigrationReplicationOnReshardingFailure()
        {
            using (var store = Sharding.GetDocumentStore())
            {
                var orchestrator = Sharding.GetOrchestrator(store.Database);

                await using (var stream = typeof(RavenDB_10404).Assembly.GetManifestResourceStream("SlowTests.Data.RavenDB_22200.OneDamagedBucket.ravendbdump"))
                {
                    var operation = await store.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), stream, CancellationToken.None);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));
                }

                const string docId = "foo/1126127153";
                using (var session = store.OpenAsyncSession())
                {
                    var exists = await session.Advanced.ExistsAsync(docId);
                    Assert.True(exists);
                }

                var notificationsQueue = new AsyncQueue<DynamicJsonValue>();
                using (orchestrator.NotificationCenter.TrackActions(notificationsQueue, null))
                {
                    var databaseRecord = store.Maintenance.ForDatabase(store.Database).Server.Send(new GetDatabaseRecordOperation(store.Database));
                    var db1 = await GetDocumentDatabaseInstanceForAsync(store, RavenDatabaseMode.Sharded, docId);
                    db1.DocumentsStorage.ForTestingPurposesOnly().DisableDebugAssertionForTableThrowNotOwned = true;

                    int fromShard, fromBucket, toShard;
                    using (db1.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                    {
                        (fromShard, fromBucket) = ShardHelper.GetShardNumberAndBucketFor(databaseRecord.Sharding, ctx.Allocator, docId);
                        toShard = ShardingTestBase.GetNextSortedShardNumber(databaseRecord.Sharding.Shards, fromShard);
                    }

                    // resharding of 10 buckets
                    await store.Maintenance.SendAsync(new StartManualReshardingOperation(fromBucket, fromBucket + 10, toShard: toShard));

                    Tuple<bool, DynamicJsonValue> alertRaised = null;
                    await AssertWaitForValueAsync(async () =>
                    {
                        alertRaised = await notificationsQueue.TryDequeueAsync(TimeSpan.FromMinutes(1));

                        if (alertRaised == null ||
                            string.Compare(alertRaised.Item2["Type"].ToString(), NotificationType.AlertRaised.ToString(), StringComparison.OrdinalIgnoreCase) != 0 ||
                            alertRaised.Item2[nameof(AlertRaised.Title)] == null ||
                            string.Compare(alertRaised.Item2[nameof(AlertRaised.Title)].ToString(), "Resharding Delay Due to an Error",
                                StringComparison.OrdinalIgnoreCase) != 0)
                            return false;

                        return true;
                    }, true, 60_000);

                    Assert.NotNull(alertRaised);
                    var msg = alertRaised.Item2[nameof(AlertRaised.Message)]?.ToString();
                    Assert.NotNull(msg);

                    var expectedMsg = $"An error occurred while attempting to clean up bucket '{fromBucket}' from source shard '{fromShard}' [{db1.Name}].";
                    Assert.Equal(expectedMsg, msg);
                }
            }
        }
    }
}
