using Raven.Client.ServerWide.Sharding;
using Raven.Server.Documents.Subscriptions;
using Raven.Server.Documents.Subscriptions.Processor;
using Raven.Server.Documents.TcpHandlers;
using Raven.Server.ServerWide;
using Raven.Server.Utils;
using Sparrow.Server;
using Sparrow.Threading;
using Sparrow.Utils;

namespace Raven.Server.Documents.Sharding.Subscriptions;

public class ShardedRevisionsDatabaseSubscriptionProcessor : RevisionsDatabaseSubscriptionProcessor
{
    private readonly ShardedDocumentDatabase _database;
    private ShardingConfiguration _sharding;
    private readonly ByteStringContext _allocator;

    public ShardedRevisionsDatabaseSubscriptionProcessor(ServerStore server, ShardedDocumentDatabase database, SubscriptionConnection connection) : base(server, database, connection)
    {
        _database = database;
        _allocator = new ByteStringContext(SharedMultipleUseFlag.None);
    }

    protected override SubscriptionFetcher<(Document Previous, Document Current)> CreateFetcher()
    {
        _sharding = _database.ShardingConfiguration;
        return base.CreateFetcher();
    }

    protected override SubscriptionBatchItem ShouldSend((Document Previous, Document Current) item, out string reason)
    {
        DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Egor, DevelopmentHelper.Severity.Normal, "https://issues.hibernatingrhinos.com/issue/RavenDB-18881/Sharding-Subscription-Revisions");
        
        var shard = ShardHelper.GetShardNumberFor(_sharding, _allocator, item.Current.Id);
        if (shard != _database.ShardNumber)
        {
            reason = $"The owner of {item.Current.Id} is shard {shard} ({_database.ShardNumber})";
            return new SubscriptionBatchItem
            {
                Document = item.Current,
                FetchingFrom = Fetcher.FetchingFrom,
                Status = SubscriptionBatchItemStatus.Skip
            };
        }

        return base.ShouldSend(item, out reason);
    }

    public override void Dispose()
    {
        base.Dispose();

        _allocator?.Dispose();
    }
}
