using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Exceptions.Sharding;
using Raven.Server.Documents.Handlers.Processors.Subscriptions;
using Raven.Server.Documents.TcpHandlers;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Subscriptions
{
    internal sealed class ShardedSubscriptionsHandlerProcessorForPutSubscription : AbstractSubscriptionsHandlerProcessorForPutSubscription<ShardedSubscriptionsHandler, TransactionOperationContext>
    {
        public ShardedSubscriptionsHandlerProcessorForPutSubscription([NotNull] ShardedSubscriptionsHandler requestHandler) : base(requestHandler)
        {
        }

        public override SubscriptionConnection.ParsedSubscription ParseSubscriptionQuery(string query)
        {
            var parsed = base.ParseSubscriptionQuery(query);
            if (parsed.Revisions)
            {
                // RavenDB-18881
                throw new NotSupportedInShardingException(@"Revisions subscription is not supported for sharded database.");
            }

            return parsed;
        }

        protected override async ValueTask CreateInternalAsync(BlittableJsonReaderObject bjro, SubscriptionCreationOptions options, TransactionOperationContext context, long? id, bool? disabled)
        {
            var sub = ParseSubscriptionQuery(options.Query);
            await RequestHandler.CreateSubscriptionInternalAsync(bjro, id, disabled, options, context, sub);
        }
    }
}
