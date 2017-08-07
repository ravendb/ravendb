using System.Net.Http;
using Raven.Client.Http;

namespace Raven.Client.Documents.Commands
{
    public class DeleteSubscriptionCommand : RavenCommand
    {
        private readonly string _name;

        public DeleteSubscriptionCommand(string name)
        {
            _name = name;
        }

        public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
        {
            url = $"{node.Url}/databases/{node.Database}/subscriptions?taskName={_name}";

            var request = new HttpRequestMessage
            {
                Method = HttpMethod.Delete
            };
            return request;
        }
    }
}