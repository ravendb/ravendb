using System.Net.Http;
using Raven.NewClient.Client.Http;
using Raven.NewClient.Client.Json;
using Sparrow.Json;

namespace Raven.NewClient.Client.Commands
{
    public class GetSubscriptionsCommand : RavenCommand<GetSubscriptionsResult>
    {
        public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
        {
            url = $"{node.Url}/databases/{node.Database}/subscriptions";
            IsReadRequest = false;

            var request = new HttpRequestMessage
            {
                Method = HttpMethod.Get,
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
            Result = JsonDeserializationClient.GetSubscriptionsResult(response);
        }
    }
}