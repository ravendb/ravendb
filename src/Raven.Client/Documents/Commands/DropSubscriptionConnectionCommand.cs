using System.Net.Http;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Client.Documents.Commands
{
    public class DropSubscriptionConnectionCommand:RavenCommand
    {
        private readonly string _name;

        public DropSubscriptionConnectionCommand(string name)
        {
            _name = name;
        }
        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            url = $"{node.Url}/databases/{node.Database}/subscriptions/drop?name={_name}";

            var request = new HttpRequestMessage
            {
                Method = HttpMethod.Post
            };
            return request;
        }
    }
}
