using System;
using System.Collections.Generic;
using Raven.Client.ServerWide.Sharding;
using Raven.Server.Documents.Subscriptions;
using Raven.Server.Documents.Subscriptions.SubscriptionProcessor;
using Raven.Server.Documents.TcpHandlers;
using Raven.Server.ServerWide;
using Raven.Server.Utils;

namespace Raven.Server.Documents.Sharding.Subscriptions;

public class ShardedDocumentsSubscriptionProcessor : DocumentsSubscriptionProcessor
{
    private readonly ShardedDocumentDatabase _database;
    private List<ShardBucketRange> _ranges;

    public ShardedDocumentsSubscriptionProcessor(ServerStore server, ShardedDocumentDatabase database, SubscriptionConnection connection) : base(server, database, connection)
    {
        _database = database;
    }

    protected override SubscriptionFetcher<Document> CreateFetcher()
    {
        _ranges = _database.ReadShardingState();
        return base.CreateFetcher();
    }

    protected override bool ShouldSend(Document item, out string reason, out Exception exception, out Document result)
    {
        exception = null;
        result = item;

        var bucket = ShardHelper.GetBucket(item.Id);
        var shard = ShardHelper.GetShardNumber(_ranges, bucket);
        if (shard != _database.ShardNumber)
        {
            reason = $"The owner of {item.Id} is shard {shard} (current shard number: {_database.ShardNumber})";
            return false;
        }

        return base.ShouldSend(item, out reason, out exception, out result);
    }
}
