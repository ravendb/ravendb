using System.Net;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Exceptions.Documents.Subscriptions;
using Raven.Server.Json;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Subscriptions
{
    internal abstract class AbstractSubscriptionsHandlerProcessorForPostSubscription<TRequestHandler, TOperationContext> : AbstractDatabaseHandlerProcessor<TRequestHandler, TOperationContext>
        where TOperationContext : JsonOperationContext
        where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
    {
        public AbstractSubscriptionsHandlerProcessorForPostSubscription([NotNull] TRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected abstract ValueTask CreateSubscriptionInternalAsync(BlittableJsonReaderObject bjro, long? id, bool? disabled, SubscriptionCreationOptions options, TransactionOperationContext context);

        public override async ValueTask ExecuteAsync()
        {
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                var json = await context.ReadForMemoryAsync(RequestHandler.RequestBodyStream(), null);
                var options = JsonDeserializationServer.SubscriptionUpdateOptions(json);
                var id = options.Id;

                SubscriptionState state;
                
                try
                {
                    if (id == null)
                    {
                        state = ServerStore.Cluster.Subscriptions.ReadSubscriptionStateByName(context, RequestHandler.DatabaseName, options.Name);
                        id = state.SubscriptionId;
                    }
                    else
                    {
                        state = ServerStore.Cluster.Subscriptions.ReadSubscriptionStateById(context, RequestHandler.DatabaseName, id.Value);
                        
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
                            await CreateSubscriptionInternalAsync(json, id: null, disabled: false, options, context);
                            return;
                        }

                        if (options.Name == null)
                        {
                            // subscription with such id doesn't exist, add new subscription using id
                            await CreateSubscriptionInternalAsync(json, id, disabled: false, options, context);
                            return;
                        }

                        // this is the case when we have both name and id, and there no subscription with such id
                        try
                        {
                            // check the name
                            state = ServerStore.Cluster.Subscriptions.ReadSubscriptionStateByName(context, RequestHandler.DatabaseName, options.Name);
                            id = state.SubscriptionId;
                        }
                        catch (SubscriptionDoesNotExistException)
                        {
                            // subscription with such id or name doesn't exist, add new subscription using both name and id
                            await CreateSubscriptionInternalAsync(json, id, disabled: false, options, context);
                            return;
                        }
                    }
                    else
                    {
                        throw;
                    }
                }

                if (options.ChangeVector == null)
                    options.ChangeVector = state.ChangeVectorForNextBatchStartingPoint;

                if (options.MentorNode == null)
                    options.MentorNode = state.MentorNode;

                if (options.Query == null)
                    options.Query = state.Query;

                if (SubscriptionsHandler.SubscriptionHasChanges(options, state) == false)
                {
                    HttpContext.Response.StatusCode = (int)HttpStatusCode.NotModified;
                    return;
                }

                await CreateSubscriptionInternalAsync(json, id, disabled: false, options, context);
            }
        }
    }
}
