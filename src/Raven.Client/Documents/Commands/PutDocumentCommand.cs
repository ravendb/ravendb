using System;
using System.Net.Http;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Serialization;
using Sparrow.Json;

namespace Raven.Client.Documents.Commands
{
    public class PutDocumentCommand : RavenCommand<PutResult>
    {
        private readonly string _id;
        private readonly string _changeVector;
        private readonly BlittableJsonReaderObject _document;

        public PutDocumentCommand(string id, string changeVector, BlittableJsonReaderObject document)
        {
            _id = id ?? throw new ArgumentNullException(nameof(id));
            _changeVector = changeVector;
            _document = document ?? throw new ArgumentNullException(nameof(document));
        }

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            url = $"{node.Url}/databases/{node.Database}/docs?id={UrlEncode(_id)}";

            var request = new HttpRequestMessage
            {
                Method = HttpMethod.Put,
                Content = new BlittableJsonContent(async stream => await ctx.WriteAsync(stream, _document).ConfigureAwait(false))
            };
            AddChangeVectorIfNotNull(_changeVector, request);
            return request;
        }

        public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
        {
            Result = JsonDeserializationClient.PutResult(response);
        }

        public override bool IsReadRequest => false;
    }
}
