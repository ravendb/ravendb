using System.Net.Http;
using Raven.NewClient.Abstractions.Data;
using Raven.NewClient.Client.Blittable;
using Raven.NewClient.Client.Data;
using Raven.NewClient.Client.Http;
using Raven.NewClient.Client.Json;
using Sparrow.Json;

namespace Raven.NewClient.Client.Commands
{
    public class CreateSubscriptionCommand : RavenCommand<CreateSubscriptionResult>
    {
        public JsonOperationContext Context;
        public long StartEtag;
        public SubscriptionCriteria Criteria;

        public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
        {
            url = $"{node.Url}/databases/{node.Database}/subscriptions/create?startEtag={StartEtag}";
            var request = new HttpRequestMessage
            {
                Method = HttpMethod.Post,
                Content = new BlittableJsonContent(stream =>
                {
                    using (var writer = new BlittableJsonTextWriter(Context, stream))
                    {
                        writer.WriteStartObject();
                        writer.WritePropertyName("Collection");
                        writer.WriteString(Criteria.Collection);
                        writer.WriteComma();
                        writer.WritePropertyName("FilterJavaScript");
                        writer.WriteString(Criteria.FilterJavaScript);
                        writer.WriteEndObject();
                    }
                })
            };
            return request;
        }

        public override void SetResponse(BlittableJsonReaderObject response)
        {
            Result = JsonDeserializationClient.CreateSubscriptionResult(response);
        }
    }
}