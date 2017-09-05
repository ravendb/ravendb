using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Text;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Converters;
using Raven.Client.ServerWide.Operations;
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
            url = $"{node.Url}/databases/{node.Database}/subscriptions/state?name={_subscriptionName}";

            var request = new HttpRequestMessage
            {
                Method = HttpMethod.Get
            };
            return request;
        }

        public override void SetResponse(BlittableJsonReaderObject response, bool fromCache)
        {
            Result = JsonDeserializationClient.SubscriptionState(response);
        }
    }
}
