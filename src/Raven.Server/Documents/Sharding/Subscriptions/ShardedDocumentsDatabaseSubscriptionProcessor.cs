using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;
using Raven.Client.ServerWide.Sharding;
using Raven.Server.Documents.Includes;
using Raven.Server.Documents.Includes.Sharding;
using Raven.Server.Documents.Subscriptions;
using Raven.Server.Documents.Subscriptions.Processor;
using Raven.Server.Documents.Subscriptions.Sharding;
using Raven.Server.Documents.Subscriptions.Stats;
using Raven.Server.Documents.TcpHandlers;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Server;
using Sparrow.Threading;
using static Raven.Server.Documents.Subscriptions.SubscriptionFetcher;

namespace Raven.Server.Documents.Sharding.Subscriptions;

public sealed class ShardedDocumentsDatabaseSubscriptionProcessor : DocumentsDatabaseSubscriptionProcessor
{
    private readonly ShardedDocumentDatabase _database;
    private ShardingConfiguration _sharding;
    private readonly ByteStringContext _allocator;

    public ShardedDocumentsDatabaseSubscriptionProcessor(ServerStore server, ShardedDocumentDatabase database, SubscriptionConnection connection) : base(server, database, connection)
    {
        _database = database;
        _allocator = new ByteStringContext(SharedMultipleUseFlag.None);
    }

    protected override SubscriptionFetcher<Document> CreateFetcher()
    {
        _sharding = _database.ShardingConfiguration;
        return new ShardDocumentSubscriptionFetcher(Database, SubscriptionConnectionsState, Collection);
    }

    protected override ConflictStatus GetConflictStatus(string changeVector)
    {
        SubscriptionState.ShardingState.ChangeVectorForNextBatchStartingPointPerShard.TryGetValue(_database.Name, out var cv);
        var conflictStatus = ChangeVectorUtils.GetConflictStatus(
            remoteAsString: changeVector,
            localAsString: cv);
        return conflictStatus;
    }

    protected override bool CanContinueBatch(SubscriptionBatchItemStatus batchItemStatus, SubscriptionBatchStatsScope batchScope, int numberOfDocs, Stopwatch sendingCurrentBatchStopwatch)
    {
        if (batchItemStatus == SubscriptionBatchItemStatus.ActiveMigration)
            return false;

        return base.CanContinueBatch(batchItemStatus, batchScope, numberOfDocs, sendingCurrentBatchStopwatch);
    }

    protected override SubscriptionBatchStatus SetBatchStatus(SubscriptionBatchResult result)
    {
        if (result.Status == SubscriptionBatchStatus.ActiveMigration)
            return SubscriptionBatchStatus.ActiveMigration;

        return base.SetBatchStatus(result);
    }

    protected override void HandleBatchItem(SubscriptionBatchStatsScope batchScope, SubscriptionBatchItem batchItem, SubscriptionBatchResult result, Document item)
    {
        if (batchItem.Status == SubscriptionBatchItemStatus.ActiveMigration)
        {
            batchItem.Document.Dispose();
            batchItem.Document.Data = null;
            result.Status = SubscriptionBatchStatus.ActiveMigration;
            return;
        }

        base.HandleBatchItem(batchScope, batchItem, result, item);
    }

    protected override string SetLastChangeVectorInThisBatch(IChangeVectorOperationContext context, string currentLast, SubscriptionBatchItem batchItem)
    {
        if (batchItem.FetchingFrom == SubscriptionFetcher.FetchingOrigin.Resend) // got this document from resend
        {
            if (batchItem.Status == SubscriptionBatchItemStatus.Skip)
                return currentLast;

            // shard might read only from resend 
        }

        var vector = context.GetChangeVector(batchItem.Document.ChangeVector);

        var result = ChangeVectorUtils.MergeVectors(
            currentLast,
            ChangeVectorUtils.NewChangeVector(_database.ServerStore.NodeTag, batchItem.Document.Etag, _database.DbBase64Id),
            vector.Order);

        return result;
    }

    protected override SubscriptionBatchItem ShouldSend(Document item, out string reason)
    {
        if (IsUnderActiveMigration(item.Id, _sharding, _allocator, _database.ShardNumber, Fetcher.FetchingFrom, out reason, out var isActiveMigration))
        {
            return new SubscriptionBatchItem
            {
                Document = item,
                FetchingFrom = Fetcher.FetchingFrom,
                Status = isActiveMigration ? SubscriptionBatchItemStatus.ActiveMigration : SubscriptionBatchItemStatus.Skip
            };
        }

        return base.ShouldSend(item, out reason);
    }
  
    public static bool IsUnderActiveMigration(string id, ShardingConfiguration sharding, ByteStringContext allocator, int shardNumber, FetchingOrigin fetchingFrom, out string reason, out bool isActiveMigration)
    {
        reason = null;
        isActiveMigration = false;
        var bucket = ShardHelper.GetBucketFor(sharding, allocator, id);
        var shard = ShardHelper.GetShardNumberFor(sharding, bucket);
        if (sharding.BucketMigrations.TryGetValue(bucket, out var migration) && migration.IsActive)
        {
            reason = $"The document '{id}' from bucket '{bucket}' is under active migration and fetched from '{fetchingFrom}'.";
            if (fetchingFrom == FetchingOrigin.Storage || shard == shardNumber)
            {
                reason += " Will set IsActiveMigration to true.";
                // we pulled doc with active migration from storage or from resend list (when it belongs to us)
                isActiveMigration = true;
            }

            return true;
        }

        if (shard != shardNumber)
        {
            reason = $"The owner of '{id}' document is shard '{shard}' (current shard number: '{shardNumber}') and fetched from '{fetchingFrom}'.";
            if (fetchingFrom == FetchingOrigin.Storage)
            {
                reason += " Will set IsActiveMigration to true.";
                isActiveMigration = true;
            }

            // current shard fetched an entry from resend list that belongs to another shard
            return true;
        }

        return false;
    }

    public override void Dispose()
    {
        base.Dispose();

        _allocator?.Dispose();
    }

    protected override bool CheckIfNewerInResendList(DocumentsOperationContext context, string id, string cvInStorage, string cvInResendList)
    {
        var resendListCvIsNewer = Database.DocumentsStorage.GetConflictStatusForVersion(context, cvInResendList, cvInStorage);
        if (resendListCvIsNewer == ConflictStatus.Update)
        {
            return true;
        }

        return false;
    }

    protected override bool ShouldFetchFromResend(DocumentsOperationContext context, string id, Document item, string currentChangeVector, out string reason)
    {
        reason = null;
        if (item == null)
        {
            // the document was delete while it was processed by the client
            ItemsToRemoveFromResend.Add(id);
            reason = $"document '{id}' removed and skipped from resend";
            return false;
        }

        var cv = context.GetChangeVector(item.ChangeVector);
        if (cv.IsSingle)
            return base.ShouldFetchFromResend(context, id, item, currentChangeVector, out reason);

        item.ChangeVector = context.GetChangeVector(cv.Version, cv.Order.RemoveId(_sharding.DatabaseId, context));

        return base.ShouldFetchFromResend(context, id, item, currentChangeVector, out reason);
    }

    public HashSet<string> Skipped;

    public override async Task<long> TryRecordBatchAsync(string lastChangeVectorSentInThisBatch)
    {
        var result = await SubscriptionConnectionsState.TryRecordBatchDocumentsAsync(BatchItems, ItemsToRemoveFromResend, lastChangeVectorSentInThisBatch);
        Skipped = result.Skipped as HashSet<string>;
        return result.Index;
    }

    public override Task AcknowledgeBatchAsync(long batchId, string changeVector)
    {
        ItemsToRemoveFromResend.Clear();
        BatchItems.Clear();

        return SubscriptionConnectionsState.AcknowledgeBatchAsync(Connection.LastSentChangeVectorInThisConnection, batchId, BatchItems, command =>
        {
            command.LastKnownSubscriptionChangeVector = changeVector;
            command.LastModifiedIndex = Connection.LastModifiedIndex;
        });
    }

    protected override ShardIncludesCommandImpl CreateIncludeCommands()
    {
        var hasIncludes = TryCreateIncludesCommand(Database, DocsContext, Connection, Connection.Subscription, out IncludeCountersCommand includeCounters, out IncludeDocumentsCommand includeDocuments, out IncludeTimeSeriesCommand includeTimeSeries);
        var includes = hasIncludes ? new ShardIncludesCommandImpl(includeDocuments, includeTimeSeries, includeCounters) : null;

        return includes;
    }
}
