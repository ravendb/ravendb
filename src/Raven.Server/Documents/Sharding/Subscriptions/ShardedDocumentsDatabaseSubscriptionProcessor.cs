using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Client.ServerWide.Sharding;
using Raven.Server.Documents.Includes;
using Raven.Server.Documents.Includes.Sharding;
using Raven.Server.Documents.Subscriptions;
using Raven.Server.Documents.Subscriptions.SubscriptionProcessor;
using Raven.Server.Documents.TcpHandlers;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Server;
using Sparrow.Threading;

namespace Raven.Server.Documents.Sharding.Subscriptions;

public class ShardedDocumentsDatabaseSubscriptionProcessor : DocumentsDatabaseSubscriptionProcessor
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
        return base.CreateFetcher();
    }

    protected override ConflictStatus GetConflictStatus(Document item)
    {
        SubscriptionState.ShardingState.ChangeVectorForNextBatchStartingPointPerShard.TryGetValue(_database.Name, out var cv);
        var conflictStatus = ChangeVectorUtils.GetConflictStatus(
            remoteAsString: item.ChangeVector,
            localAsString: cv);
        return conflictStatus;
    }

    protected override bool ShouldSend(Document item, out string reason, out Exception exception, out Document result)
    {
        exception = null;
        result = item;

        if (Fetcher.FetchingFrom == SubscriptionFetcher.FetchingOrigin.Resend)
        {
            var bucket = ShardHelper.GetBucketFor(_sharding, _allocator, item.Id);
           
            if (_sharding.BucketMigrations.TryGetValue(bucket, out var migration))
            {
                if (migration.IsActive)
                {
                    reason = $"The document {item.Id} from bucket {bucket} is under active migration)";
                    item.Data = null;
                    item.ChangeVector = string.Empty;
                    return false;
                }
            }

            var shard = ShardHelper.GetShardNumberFor(_sharding, bucket);
            if (shard != _database.ShardNumber)
            {
                reason = $"The owner of {item.Id} is shard {shard} (current shard number: {_database.ShardNumber})";
                item.Data = null;
                item.ChangeVector = string.Empty;
                return false;
            }
        }

        return base.ShouldSend(item, out reason, out exception, out result);
    }

    public override void Dispose()
    {
        base.Dispose();

        _allocator?.Dispose();
    }
    protected override bool ShouldFetchFromResend(DocumentsOperationContext context, string id, DocumentsStorage.DocumentOrTombstone item, string currentChangeVector, out string reason)
    {
        reason = null;
        if (item.Document == null)
        {
            // the document was delete while it was processed by the client
            ItemsToRemoveFromResend.Add(id);
            reason = $"document '{id}' removed and skipped from resend";
            return false;
        }

        var cv = context.GetChangeVector(item.Document.ChangeVector);
        if (cv.IsSingle)
            return base.ShouldFetchFromResend(context, id, item, currentChangeVector, out reason);

        item.Document.ChangeVector = context.GetChangeVector(cv.Version, cv.Order.RemoveId(_sharding.DatabaseId, context));
        return true;
    }

    public HashSet<string> Skipped;

    public override async Task<long> RecordBatch(string lastChangeVectorSentInThisBatch)
    {
        var result = await SubscriptionConnectionsState.RecordBatchDocuments(BatchItems, ItemsToRemoveFromResend, lastChangeVectorSentInThisBatch);
        Skipped = result.Skipped as HashSet<string>;
        return result.Index;
    }

    protected override ShardIncludesCommandImpl CreateIncludeCommands()
    {
        var hasIncludes = TryCreateIncludesCommand(Database, DocsContext, Connection, Connection.Subscription, out IncludeCountersCommand includeCounters, out IncludeDocumentsCommand includeDocuments, out IncludeTimeSeriesCommand includeTimeSeries);
        var includes = hasIncludes ? new ShardIncludesCommandImpl(includeDocuments, includeTimeSeries, includeCounters) : null;

        return includes;
    }
}
