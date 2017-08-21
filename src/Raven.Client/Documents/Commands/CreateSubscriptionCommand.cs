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
        private readonly SubscriptionCreationOptions _options;

        public CreateSubscriptionCommand(SubscriptionCreationOptions options)
        {
            _options = options;
        }

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            url = $"{node.Url}/databases/{node.Database}/subscriptions";
            var request = new HttpRequestMessage
            {
                Method = HttpMethod.Put,
                Content = new BlittableJsonContent(stream =>
                {
                    ctx.Write(stream, 
                        EntityToBlittable.ConvertEntityToBlittable(_options, DocumentConventions.Default, ctx));
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