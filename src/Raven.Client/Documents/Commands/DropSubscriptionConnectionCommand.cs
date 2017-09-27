using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Text;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Client.Documents.Commands
{
    public class DropSubscriptionConnectionCommand:RavenCommand
    {
        private readonly string _id;

        public DropSubscriptionConnectionCommand(string id)
        {
            _id = id;
        }
        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            url = $"{node.Url}/databases/{node.Database}/subscriptions/drop?id={_id}";

            var request = new HttpRequestMessage
            {
                Method = HttpMethod.Post
            };
            return request;
        }
    }
}
