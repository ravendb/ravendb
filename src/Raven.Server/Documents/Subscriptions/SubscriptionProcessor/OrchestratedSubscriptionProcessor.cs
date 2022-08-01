using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.ServerWide.Sharding;
using Raven.Server.Documents.Sharding;
using Raven.Server.Documents.Sharding.Subscriptions;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Server;
using Sparrow.Threading;
using Sparrow.Utils;

namespace Raven.Server.Documents.Subscriptions.SubscriptionProcessor;

public class OrchestratedSubscriptionProcessor : SubscriptionProcessor
{
    private readonly ShardedDatabaseContext _databaseContext;
    private SubscriptionConnectionsStateOrchestrator _state;

    public ShardedSubscriptionBatch CurrentBatch;

    public OrchestratedSubscriptionProcessor(ServerStore server, ShardedDatabaseContext databaseContext, OrchestratedSubscriptionConnection connection) : base(server, connection)
    {
        _databaseContext = databaseContext;
    }

    public override void InitializeProcessor()
    {
        base.InitializeProcessor();
        _state = _databaseContext.Subscriptions.SubscriptionsConnectionsState[Connection.SubscriptionId];
    }

    // should never hit this
    public override Task<long> RecordBatch(string lastChangeVectorSentInThisBatch) => throw new NotImplementedException();

    // should never hit this
    public override Task AcknowledgeBatch(long batchId) => throw new NotImplementedException();

    protected override SubscriptionIncludeCommands CreateIncludeCommands()
    {
        DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Egor, DevelopmentHelper.Severity.Major,
            "https://issues.hibernatingrhinos.com/issue/RavenDB-16279");
        return new SubscriptionIncludeCommands();
    }

    public override IEnumerable<(Document Doc, Exception Exception)> GetBatch()
    {
        if (_state.Batches.TryTake(out CurrentBatch, TimeSpan.Zero) == false)
            yield break;
        
        using (CurrentBatch.ReturnContext)
        using (Server.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
        {
            var migrationFilter = new SubscriptionBatchMigrationHelper();
            migrationFilter.Init(Server, _databaseContext.DatabaseName, Connection.SubscriptionId, ShardHelper.GetShardNumber(CurrentBatch.ShardName));

            foreach (var batchItem in CurrentBatch.Items)
            {
                Connection.CancellationTokenSource.Token.ThrowIfCancellationRequested();

                if (batchItem.ExceptionMessage != null)
                    yield return (null, new Exception(batchItem.ExceptionMessage));
                
                using (var document = new Document
                       {
                           ChangeVector = batchItem.ChangeVector, 
                           Id = ClusterContext.GetLazyString(batchItem.Id)
                       })
                {
                    var changeVector = context.GetChangeVector(document.ChangeVector);
                    var shouldSkip = migrationFilter.ShouldSkip(context.Allocator, document.Id, changeVector, out var reason);
                    if (shouldSkip == false)
                    {
                        document.Data = batchItem.RawResult.Clone(ClusterContext);
                    }

                    if (Connection._logger.IsInfoEnabled)
                    {
                        Connection._logger.Info(reason);
                    }

                    yield return (document, null);
                }
            }
        }
    }

    private class SubscriptionBatchMigrationHelper
    {
        private string _moveId;
        private SubscriptionState _subscriptionState;
        private int _currentShard;
        private List<ShardBucketRange> _ranges;
        private Dictionary<int, ShardBucketMigration> _activeMigration;
        private HashSet<string> _exclude;

        public void Init(ServerStore server, string database, long subId, int shardNumber)
        {
            _currentShard = shardNumber;

            using (server.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                var raw = server.Cluster.ReadRawDatabaseRecord(context, database).Sharding;
                _moveId = raw.ShardedDatabaseId;
                _ranges = raw.ShardBucketRanges;
                _subscriptionState = server.Cluster.Subscriptions.ReadSubscriptionStateById(context, database, subId);
                _activeMigration = raw.BucketMigrations;
            }

            _exclude = new HashSet<string>(capacity: 1) { _moveId };
        }

        public bool ShouldSkip(ByteStringContext context, LazyStringValue id, ChangeVector changeVector, out string reason)
        {
            var bucket = ShardHelper.GetBucket(context, id);
            var actualShard = ShardHelper.GetShardNumber(_ranges, bucket);
            var moveIndex = ChangeVectorUtils.GetEtagById(changeVector.Order, _moveId);

            if (_activeMigration.TryGetValue(bucket, out var migration))
            {
                if (_currentShard != migration.DestinationShard && moveIndex == 0)
                {
                    // document from old shard, which hasn't been moved yet
                    // check whether it was put into the resend queue before we start the resharding
                    if (_subscriptionState.SubscriptionShardingState.IgnoreBucketLesserChangeVector.TryGetValue(migration.MigrationIndex, out var skipChangeVector))
                    {
                        var status = ChangeVectorUtils.GetConflictStatus(changeVector.Version, skipChangeVector);
                        if (status == ConflictStatus.Update)
                        {
                            reason =
                                $"Got '{id}' (change-vector: '{changeVector}') from shard '{_currentShard}', but it will be sent by its new owner shard '{migration.DestinationShard}', (marked change vector: {skipChangeVector}, status: {status})";
                            return true;
                        }

                        if (status == ConflictStatus.AlreadyMerged)
                        {
                            reason = $"'{id}' (change-vector: '{changeVector}') from shard '{_currentShard}' was already processed, (skip: '{skipChangeVector}', status: '{status}')";
                            return true;
                        }
                    }
                }
            }
            else
            {
                if (actualShard != _currentShard)
                {
                    // not the owner
                    reason = $"Not the owner of '{id}' (change-vector: '{changeVector}') (expected shard: '{_currentShard}', actual shard: '{actualShard}')";
                    return true;
                }
            }

            if (moveIndex > 0)
            {
                if (_subscriptionState.SubscriptionShardingState.IgnoreBucketLesserChangeVector.TryGetValue(moveIndex, out var skipChangeVector))
                {
                    var status = ChangeVectorUtils.GetConflictStatus(changeVector.Version, skipChangeVector, _exclude);
                    if (status == ConflictStatus.AlreadyMerged)
                    {
                        reason = $"'{id}' (change-vector: '{changeVector}') was already processed, (skip: '{skipChangeVector}', status: '{status}')";
                        return true;
                    }
                }
            }

            reason = $"allow '{id}' (change-vector: '{changeVector}'";
            return false;
        }
    }
}
