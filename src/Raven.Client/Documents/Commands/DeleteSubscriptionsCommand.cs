using System.Net.Http;
using Raven.Client.Http;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.Documents.Commands
{
    public class DeleteSubscriptionCommand : RavenCommand, IRaftCommand
    {
        private readonly string _name;

        public DeleteSubscriptionCommand(string name)
        {
            _name = name;
        }

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            url = $"{node.Url}/databases/{node.Database}/subscriptions?taskName={_name}";

            var request = new HttpRequestMessage
            {
                Method = HttpMethod.Delete
            };
            return request;
        }

        public string RaftUniqueRequestId { get; } = RaftIdGenerator.NewId();
    }
}
