using System.Net;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Exceptions.Documents.Subscriptions;
using Raven.Server.Documents.Subscriptions;
using Raven.Server.Documents.TcpHandlers;
using Raven.Server.Json;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Subscriptions
{
    internal abstract class AbstractSubscriptionsHandlerProcessorForPostSubscription<TRequestHandler, TOperationContext, TSubscriptionState> : AbstractDatabaseHandlerProcessor<TRequestHandler, TOperationContext>
        where TOperationContext : JsonOperationContext
        where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
        where TSubscriptionState : AbstractSubscriptionConnectionsState
    {
        protected readonly AbstractSubscriptionStorage<TSubscriptionState> SubscriptionStorage;

        protected AbstractSubscriptionsHandlerProcessorForPostSubscription([NotNull] TRequestHandler requestHandler, AbstractSubscriptionStorage<TSubscriptionState> subscriptionStorage) : base(requestHandler)
        {
            SubscriptionStorage = subscriptionStorage;
        }

        public virtual SubscriptionConnection.ParsedSubscription ParseSubscriptionQuery(string query)
        {
            return SubscriptionConnection.ParseSubscriptionQuery(query);
        }

        protected abstract ValueTask CreateSubscriptionInternalAsync(BlittableJsonReaderObject bjro, long? id, bool? disabled, SubscriptionCreationOptions options, ClusterOperationContext context);

        public override async ValueTask ExecuteAsync()
        {
            using (ServerStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
            using (context.OpenReadTransaction())
            {
                var json = await context.ReadForMemoryAsync(RequestHandler.RequestBodyStream(), null);
                bool pinToMentorNodeWasSet = json.TryGet(nameof(SubscriptionUpdateOptions.PinToMentorNode), out bool pinToMentorNode);
                bool disabledWasSet = json.TryGet(nameof(SubscriptionUpdateOptions.Disabled), out bool _);
                var options = JsonDeserializationServer.SubscriptionUpdateOptions(json);
                var id = options.Id;

                SubscriptionState state;

                try
                {
                    if (id == null)
                    {
                        state = SubscriptionStorage.GetSubscriptionByName(context, options.Name);
                        id = state.SubscriptionId;
                    }
                    else
                    {
                        state = SubscriptionStorage.GetSubscriptionById(context, id.Value);

                        // keep the old subscription name
                        if (options.Name == null)
                            options.Name = state.SubscriptionName;
                    }
                }
                catch (SubscriptionDoesNotExistException)
                {
                    if (options.CreateNew)
                    {
                        if (id == null)
                        {
                            // subscription with such name doesn't exist, add new subscription
                            await CreateSubscriptionInternalAsync(json, id: null, options.Disabled, options, context);
                            return;
                        }

                        if (options.Name == null)
                        {
                            // subscription with such id doesn't exist, add new subscription using id
                            await CreateSubscriptionInternalAsync(json, id, options.Disabled, options, context);
                            return;
                        }

                        // this is the case when we have both name and id, and there no subscription with such id
                        try
                        {
                            // check the name
                            state = SubscriptionStorage.GetSubscriptionByName(context, options.Name);
                            id = state.SubscriptionId;
                        }
                        catch (SubscriptionDoesNotExistException)
                        {
                            // subscription with such id or name doesn't exist, add new subscription using both name and id
                            await CreateSubscriptionInternalAsync(json, id, options.Disabled, options, context);
                            return;
                        }
                    }
                    else
                    {
                        throw;
                    }
                }

                SetSubscriptionChangeVectorOnUpdate(options, state);
                options.MentorNode ??= state.MentorNode;
                options.Query ??= state.Query;
                options.ArchivedDataProcessingBehavior = state.ArchivedDataProcessingBehavior;

                if (pinToMentorNodeWasSet == false)
                    options.PinToMentorNode = state.PinToMentorNode;

                if (disabledWasSet == false)
                    options.Disabled = state.Disabled;

                if (SubscriptionsHandler.SubscriptionHasChanges(options, state) == false)
                {
                    HttpContext.Response.StatusCode = (int)HttpStatusCode.NotModified;
                    return;
                }

                await CreateSubscriptionInternalAsync(json, id, options.Disabled, options, context);
            }
        }

        protected abstract void SetSubscriptionChangeVectorOnUpdate(SubscriptionUpdateOptions options, SubscriptionState state);
    }
}
