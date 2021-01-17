using System;
using System.Net.Http;
using System.Text;
using Raven.Client.Http;
using Raven.Client.Json.Serialization;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Commands
{
    public class GetRevisionsCountCommand : RavenCommand<long>
    {
        private readonly string _id;

        public GetRevisionsCountCommand(string id)
        {
            _id = id ?? throw new ArgumentNullException(nameof(id));
        }

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            var request = new HttpRequestMessage
            {
                Method = HttpMethod.Get
            };

            var pathBuilder = new StringBuilder(node.Url);
            pathBuilder.Append("/databases/")
                .Append(node.Database)
                .Append("/revisions/count?")
                .Append("&id=")
                .Append(Uri.EscapeDataString(_id));

            url = pathBuilder.ToString();
            return request;
        }

        public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
        {
            if (response == null)
            {
                Result = default;
                return;
            }

            Result = JsonDeserializationClient.DocumentRevisionsCount(response).RevisionsCount;
        }

        public override bool IsReadRequest => true;

        internal class DocumentRevisionsCount : IDynamicJson
        {
            public long RevisionsCount { get; set; }

            public DynamicJsonValue ToJson()
            {
                return new DynamicJsonValue
                {
                    [nameof(RevisionsCount)] = RevisionsCount
                };
            }
        }
    }
}
