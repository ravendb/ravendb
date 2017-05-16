using System.Net.Http;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Client.Documents.Commands
{
    public class DeleteSubscriptionCommand : RavenCommand
    {
        private readonly long _id;

        public DeleteSubscriptionCommand(long id)
        {
            _id = id;
            ResponseType = RavenCommandResponseType.Empty;
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

        public override bool IsReadRequest => false;
    }
}