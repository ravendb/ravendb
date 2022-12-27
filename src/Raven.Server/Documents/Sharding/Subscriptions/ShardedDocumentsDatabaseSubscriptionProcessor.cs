using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Nest;
using Raven.Client.ServerWide.Sharding;
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
        _sharding = _database.ReadShardingState();
        return base.CreateFetcher();
    }

    protected override bool ShouldSend(Document item, out string reason, out Exception exception, out Document result)
    {
        exception = null;
        result = item;

        if (Fetcher.FetchingFrom == SubscriptionFetcher.FetchingOrigin.Resend)
        {
            var bucket = ShardHelper.GetBucketFor(_allocator, item.Id);
            foreach (var setting in _sharding.Prefixed)
            {
                if (item.Id.StartsWith(setting.Prefix, StringComparison.OrdinalIgnoreCase))
                {
                    bucket += setting.BucketRangeStart;
                    break;
                }
            }
            if (_sharding.BucketMigrations.TryGetValue(bucket, out var migration))
            {
                if (migration.Status < MigrationStatus.OwnershipTransferred)
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
}
