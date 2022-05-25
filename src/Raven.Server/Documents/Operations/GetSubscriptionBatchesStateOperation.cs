using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Http;
using Raven.Server.Documents.Handlers.Processors.Subscriptions;
using Raven.Server.Json;
using Sparrow.Json;

namespace Raven.Server.Documents.Operations
{
    public class GetSubscriptionBatchesStateOperation : IMaintenanceOperation<SubscriptionBatchesState>
    {
        private readonly string _subscriptionName;

        public GetSubscriptionBatchesStateOperation(string subscriptionName)
        {
            _subscriptionName = subscriptionName;
        }
        public RavenCommand<SubscriptionBatchesState> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new GetSubscriptionBatchesStateCommand(_subscriptionName);
        }

        internal class GetSubscriptionBatchesStateCommand : RavenCommand<SubscriptionBatchesState>
        {
            private readonly string _subscriptionName;

            public GetSubscriptionBatchesStateCommand(string subscriptionName)
            {
                _subscriptionName = subscriptionName;
            }

            public override bool IsReadRequest => true;

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/debug/subscriptions/resend?name={Uri.EscapeDataString(_subscriptionName)}";

                var request = new HttpRequestMessage { Method = HttpMethod.Get };

                return request;
            }

            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                    return;

                Result = JsonDeserializationServer.SubscriptionBatchesState(response);
            }
        }
    }
}
