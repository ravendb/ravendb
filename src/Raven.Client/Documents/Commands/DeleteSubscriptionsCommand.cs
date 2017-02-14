using System.Net.Http;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Http;
using Raven.Client.Json.Converters;
using Sparrow.Json;

namespace Raven.Client.Documents.Commands
{
    public class DeleteSubscriptionCommand : RavenCommand<CreateSubscriptionResult>
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

        public override void SetResponse(BlittableJsonReaderObject response, bool fromCache)
        {
            if (response == null)
            {
                Result = null;
                return;
            }
            Result = JsonDeserializationClient.CreateSubscriptionResult(response);
        }

        public override bool IsReadRequest => false;
    }
}