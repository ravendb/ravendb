using System.Collections.Generic;
using System.Net;
using JetBrains.Annotations;
using Raven.Client.Documents.Subscriptions;
using Raven.Server.Documents.Subscriptions;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Processors.Subscriptions
{
    internal class SubscriptionsHandlerProcessorForGetSubscription : AbstractSubscriptionsHandlerProcessorForGetSubscription<DatabaseRequestHandler, DocumentsOperationContext>
    {
        public SubscriptionsHandlerProcessorForGetSubscription([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override IEnumerable<SubscriptionState> GetSubscriptions(TransactionOperationContext context, int start, int pageSize, bool history, bool running, long? id, string name)
        {
            IEnumerable<SubscriptionStorage.SubscriptionGeneralDataAndStats> subscriptions;
            if (string.IsNullOrEmpty(name) && id == null)
            {
                subscriptions = running
                    ? RequestHandler.Database.SubscriptionStorage.GetAllRunningSubscriptions(context, history, start, pageSize)
                    : RequestHandler.Database.SubscriptionStorage.GetAllSubscriptions(context, history, start, pageSize);
            }
            else
            {
                var subscription = running
                    ? RequestHandler.Database
                        .SubscriptionStorage
                        .GetRunningSubscription(context, id, name, history)
                    : RequestHandler.Database
                        .SubscriptionStorage
                        .GetSubscription(context, id, name, history);

                if (subscription == null)
                {
                    HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                    return null;
                }

                subscriptions = new[] {subscription};
            }
            return subscriptions;
        }
    }
}
