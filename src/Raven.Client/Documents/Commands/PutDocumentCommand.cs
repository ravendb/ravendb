using System.Net.Http;
using Raven.Abstractions.Data;
using Raven.Client.Json;
using Sparrow.Json;

namespace Raven.Client.Documents.Commands
{
    public class PutDocumentCommand : RavenCommand<PutResult>
    {
        public string Id;
        public long? Etag;
        public BlittableJsonReaderObject Document;
        public JsonOperationContext Context;

        public override HttpRequestMessage CreateRequest(out string url)
        {
            EnsureIsNotNullOrEmpty(Id, nameof(Id));

            url = $"docs?id={UrlEncode(Id)}";
            IsReadRequest = false;
            var request = new HttpRequestMessage
            {
                Method = HttpMethod.Put,
                Content = new BlittableJsonContent(Document, Context),
            };

            return request;
        }

        public override void SetResponse(BlittableJsonReaderObject response)
        {
            Result = JsonDeserializationClient.PutResult(response);
        }
    }
}