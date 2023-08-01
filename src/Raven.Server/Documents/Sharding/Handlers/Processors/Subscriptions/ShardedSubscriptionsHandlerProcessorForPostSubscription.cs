using System;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Exceptions.Sharding;
using Raven.Server.Documents.Handlers.Processors.Subscriptions;
using Raven.Server.Documents.Sharding.Subscriptions;
using Raven.Server.Documents.TcpHandlers;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Subscriptions
{
    internal sealed class ShardedSubscriptionsHandlerProcessorForPostSubscription : AbstractSubscriptionsHandlerProcessorForPostSubscription<ShardedSubscriptionsHandler, TransactionOperationContext, SubscriptionConnectionsStateOrchestrator>
    {
        public ShardedSubscriptionsHandlerProcessorForPostSubscription([NotNull] ShardedSubscriptionsHandler requestHandler)
            : base(requestHandler, requestHandler.DatabaseContext.SubscriptionsStorage)
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

        protected override async ValueTask CreateSubscriptionInternalAsync(BlittableJsonReaderObject bjro, long? id, bool? disabled, SubscriptionCreationOptions options, ClusterOperationContext context)
        {
            var sub = ParseSubscriptionQuery(options.Query);
            await RequestHandler.CreateSubscriptionInternalAsync(bjro, id, disabled, options, context, sub);
        }

        protected override void SetSubscriptionChangeVectorOnUpdate(SubscriptionUpdateOptions options, SubscriptionState state)
        {
            // the actual validation will happen in TryValidateChangeVector
            if (string.IsNullOrEmpty(options.ChangeVector))
            {
                options.ChangeVector = nameof(Constants.Documents.SubscriptionChangeVectorSpecialStates.DoNotChange);
            }
        }
    }
}
