using System;
using Raven.Client.ServerWide.Sharding;
using Raven.Server.Documents.Subscriptions;
using Raven.Server.Documents.Subscriptions.SubscriptionProcessor;
using Raven.Server.Documents.TcpHandlers;
using Raven.Server.ServerWide;
using Raven.Server.Utils;

namespace Raven.Server.Documents.Sharding.Subscriptions;

public class ShardedRevisionsDatabaseSubscriptionProcessor : RevisionsDatabaseSubscriptionProcessor
{
    private readonly ShardedDocumentDatabase _database;
    private ShardingConfiguration _sharding;

    public ShardedRevisionsDatabaseSubscriptionProcessor(ServerStore server, ShardedDocumentDatabase database, SubscriptionConnection connection) : base(server, database, connection)
    {
    }
    protected override SubscriptionFetcher<(Document Previous, Document Current)> CreateFetcher()
    {
        _sharding = _database.ReadShardingState();
        return base.CreateFetcher();
    }

    protected override bool ShouldSend((Document Previous, Document Current) item, out string reason, out Exception exception, out Document result)
    {
        exception = null;
        result = item.Current;

        var bucket = ShardHelper.GetBucket(result.Id);
        var shard = ShardHelper.GetShardNumber(_sharding.BucketRanges, bucket);
        if (shard != _database.ShardNumber)
        {
            reason = $"The owner of {result.Id} is shard {shard} ({_database.ShardNumber})";
            return false;
        }

        return base.ShouldSend(item, out reason, out exception, out result);
    }
}
