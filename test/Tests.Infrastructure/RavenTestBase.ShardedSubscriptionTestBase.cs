using System;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Server.Documents.Subscriptions;
using Raven.Server.ServerWide.Commands.Subscriptions;
using Raven.Server.ServerWide.Context;
using Xunit;

namespace FastTests;

public partial class RavenTestBase
{
    public class ShardedSubscriptionTestBase
    {
        private readonly RavenTestBase _parent;

        public ShardedSubscriptionTestBase(RavenTestBase parent)
        {
            _parent = parent ?? throw new ArgumentNullException(nameof(parent));
        }

        public async Task AssertNoItemsInTheResendQueueAsync(IDocumentStore store, string subscriptionId)
        {
            var id = long.Parse(subscriptionId);
            var shards = _parent.Sharding.GetShardsDocumentDatabaseInstancesFor(store);
            await foreach (var shard in shards)
            {
                var result = await WaitForValueAsync(() =>
                {
                    using (shard.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
                    using (ctx.OpenReadTransaction())
                    {
                        return Task.FromResult(AbstractSubscriptionConnectionsState.GetNumberOfResendDocuments(shard.ServerStore, store.Database, SubscriptionType.Document, id));
                    }
                }, 0, timeout: 1_500_000);
                // SubscriptionConnectionsStateBase.TryGetDocumentFromResend()
                using (shard.ServerStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext ctx))
                using (ctx.OpenReadTransaction())
                {
                    Assert.True(result == 0, $"{string.Join(Environment.NewLine, AbstractSubscriptionConnectionsState.GetResendItems(ctx, store.Database, id).Select(x=>$"{x.Id}, {x.ChangeVector}, {x.Batch}"))}");
                }
            }
        }
    }
}
