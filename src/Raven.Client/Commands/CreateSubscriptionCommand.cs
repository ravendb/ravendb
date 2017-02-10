using System.Net.Http;
using Raven.Client.Blittable;
using Raven.Client.Data;
using Raven.Client.Http;
using Raven.Client.Json;
using Sparrow.Json;

namespace Raven.Client.Commands
{
    public class CreateSubscriptionCommand : RavenCommand<CreateSubscriptionResult>
    {
        public JsonOperationContext Context;
        public long StartEtag;
        public SubscriptionCriteria Criteria;

        public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
        {
            url = $"{node.Url}/databases/{node.Database}/subscriptions?startEtag={StartEtag}";
            var request = new HttpRequestMessage
            {
                Method = HttpMethod.Put,
                Content = new BlittableJsonContent(stream =>
                {
                    using (var writer = new BlittableJsonTextWriter(Context, stream))
                    {
                        writer.WriteStartObject();
                        writer.WritePropertyName(nameof(SubscriptionCriteria.Collection));
                        writer.WriteString(Criteria.Collection);
                        writer.WriteComma();
                        writer.WritePropertyName(nameof(SubscriptionCriteria.FilterJavaScript));
                        writer.WriteString(Criteria.FilterJavaScript);
                        writer.WriteEndObject();
                    }
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