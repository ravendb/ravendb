using System;
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
        private readonly string _id;
        private readonly long? _etag;
        private readonly BlittableJsonReaderObject _document;
        private readonly JsonOperationContext _context;

        public PutDocumentCommand(string id, long? etag, BlittableJsonReaderObject document, JsonOperationContext context)
        {
            _id = id ?? throw new ArgumentNullException(nameof(id));
            _etag = etag;
            _document = document ?? throw new ArgumentNullException(nameof(document));
            _context = context ?? throw new ArgumentNullException(nameof(context));
        }

        public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
        {
            url = $"{node.Url}/databases/{node.Database}/docs?id={UrlEncode(_id)}";

            var request = new HttpRequestMessage
            {
                Method = HttpMethod.Put,
                Content = new BlittableJsonContent(stream =>
                {
                    _context.Write(stream, _document);
                }),
            };
            AddEtagIfNotNull(_etag, request);
            return request;
        }

        public override void SetResponse(BlittableJsonReaderObject response, bool fromCache)
        {
            Result = JsonDeserializationClient.PutResult(response);
        }

        public override bool IsReadRequest => false;
    }
}