using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Converters;
using Sparrow.Json;

namespace Raven.Client.Documents.Commands
{
    public class CreateSubscriptionCommand : RavenCommand<CreateSubscriptionResult>
    {
        public JsonOperationContext Context;
        public SubscriptionCreationParams SubscriptionCreationParams;

        public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
        {
            url = $"{node.Url}/databases/{node.Database}/subscriptions";
            var request = new HttpRequestMessage
            {
                Method = HttpMethod.Put,
                Content = new BlittableJsonContent(stream =>
                {
                    Context.Write(stream, 
                        EntityToBlittable.ConvertEntityToBlittable(SubscriptionCreationParams, DocumentConventions.Default, Context));
                })
            };
            return request;
        }

        public override void SetResponse(BlittableJsonReaderObject response, bool fromCache)
        {
            Result = JsonDeserializationClient.CreateSubscriptionResult(response);
        }

        public override bool IsReadRequest => false;
    }
}