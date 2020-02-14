using System;
using System.Net.Http;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Http;
using Raven.Client.Json.Converters;
using Sparrow.Json;

namespace Raven.Client.Documents.Commands
{
    public class GetSubscriptionStateCommand: RavenCommand<SubscriptionState>
    {
        private readonly string _subscriptionName;
        public override bool IsReadRequest => true;

        public GetSubscriptionStateCommand(string subscriptionName)
        {
            _subscriptionName = subscriptionName;
        }
        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            url = $"{node.Url}/databases/{node.Database}/subscriptions/state?name={Uri.EscapeDataString(_subscriptionName)}";

            var request = new HttpRequestMessage
            {
                Method = HttpMethod.Get
            };
            return request;
        }

        public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
        {
            Result = JsonDeserializationClient.SubscriptionState(response);
        }
    }
}
