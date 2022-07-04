using System;
using Raven.Client.ServerWide.Sharding;
using Raven.Server.Documents.Subscriptions;
using Raven.Server.Documents.Subscriptions.SubscriptionProcessor;
using Raven.Server.Documents.TcpHandlers;
using Raven.Server.ServerWide;
using Raven.Server.Utils;

namespace Raven.Server.Documents.Sharding.Subscriptions;

public class ShardedDocumentsDatabaseSubscriptionProcessor : DocumentsDatabaseSubscriptionProcessor
{
    private readonly ShardedDocumentDatabase _database;
    private ShardingConfiguration _sharding;

    public ShardedDocumentsDatabaseSubscriptionProcessor(ServerStore server, ShardedDocumentDatabase database, SubscriptionConnection connection) : base(server, database, connection)
    {
        _database = database;
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
            var bucket = ShardHelper.GetBucket(item.Id);
            var shard = ShardHelper.GetShardNumber(_sharding.BucketRanges, bucket);
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
}
