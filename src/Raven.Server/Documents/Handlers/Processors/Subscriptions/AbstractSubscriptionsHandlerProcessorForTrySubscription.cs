using System;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Subscriptions;
using Raven.Server.Documents.TcpHandlers;
using Raven.Server.Json;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Subscriptions
{
    internal abstract class AbstractSubscriptionsHandlerProcessorForTrySubscription<TRequestHandler, TOperationContext> : AbstractDatabaseHandlerProcessor<TRequestHandler, TOperationContext>
        where TOperationContext : JsonOperationContext
        where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
    {
        protected AbstractSubscriptionsHandlerProcessorForTrySubscription([NotNull] TRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected abstract ValueTask TryoutSubscriptionAsync(TOperationContext context, SubscriptionConnection.ParsedSubscription subscription, SubscriptionTryout tryout, int pageSize);

        public override async ValueTask ExecuteAsync()
        {
            using (ContextPool.AllocateOperationContext(out TOperationContext context))
            {
                using var json = await context.ReadForMemoryAsync(RequestHandler.RequestBodyStream(), null);
                var tryout = JsonDeserializationServer.SubscriptionTryout(json);

                var sub = SubscriptionConnection.ParseSubscriptionQuery(tryout.Query);

                if (sub.Collection == null)
                    throw new ArgumentException("Collection must be specified");

                const int maxPageSize = 1024;
                var pageSize = RequestHandler.GetIntValueQueryString("pageSize") ?? 1;
                if (pageSize > maxPageSize)
                    throw new ArgumentException($"Cannot gather more than {maxPageSize} results during tryouts, but requested number was {pageSize}.");

                await TryoutSubscriptionAsync(context, sub, tryout, pageSize);
            }
        }
    }
}
