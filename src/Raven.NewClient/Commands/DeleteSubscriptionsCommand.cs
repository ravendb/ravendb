using System.Net.Http;
using Raven.NewClient.Client.Http;
using Raven.NewClient.Client.Json;
using Sparrow.Json;

namespace Raven.NewClient.Client.Commands
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
            IsReadRequest = false;

            var request = new HttpRequestMessage
            {
                Method = HttpMethod.Delete,
            };
            return request;
        }

        public override void SetResponse(BlittableJsonReaderObject response)
        {
            if (response == null)
            {
                Result = null;
                return;
            }
            Result = JsonDeserializationClient.CreateSubscriptionResult(response);
        }
    }
}