using System.Net.Http;
using Raven.Client.Http;

namespace Raven.Client.Documents.Commands
{
    public class DeleteSubscriptionCommand : RavenCommand
    {
        private readonly long _id;

        public DeleteSubscriptionCommand(long id)
        {
            _id = id;
        }

        public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
        {
            url = $"{node.Url}/databases/{node.Database}/subscriptions?id={_id}";

            var request = new HttpRequestMessage
            {
                Method = HttpMethod.Delete,
            };
            return request;
        }
    }
}