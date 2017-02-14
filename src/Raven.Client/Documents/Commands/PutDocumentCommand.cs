using System.Net.Http;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Converters;
using Sparrow.Json;

namespace Raven.Client.Documents.Commands
{
    public class PutDocumentCommand : RavenCommand<PutResult>
    {
        public string Id;
        public long? Etag;
        public BlittableJsonReaderObject Document;
        public JsonOperationContext Context;

        public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
        {
            EnsureIsNotNullOrEmpty(Id, nameof(Id));

            url = $"{node.Url}/databases/{node.Database}/docs?id={UrlEncode(Id)}";

            var request = new HttpRequestMessage
            {
                Method = HttpMethod.Put,
                Content = new BlittableJsonContent(stream =>
                {
                    Context.Write(stream, Document);
                }),
            };

            return request;
        }

        public override void SetResponse(BlittableJsonReaderObject response, bool fromCache)
        {
            Result = JsonDeserializationClient.PutResult(response);
        }

        public override bool IsReadRequest => false;
    }
}