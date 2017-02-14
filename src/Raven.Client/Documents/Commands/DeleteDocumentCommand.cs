using System;
using System.Net.Http;
using System.Net.Http.Headers;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Client.Documents.Commands
{
    public class DeleteDocumentCommand : RavenCommand<object>
    {
        private readonly string _id;
        private readonly long? _etag;

        public DeleteDocumentCommand(string id, long? etag)
        {
            if (id == null)
                throw new ArgumentNullException(nameof(id));

            _id = id;
            _etag = etag;
        }

        public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
        {
            EnsureIsNotNullOrEmpty(_id, nameof(_id));

            url = $"{node.Url}/databases/{node.Database}/docs?id={UrlEncode(_id)}";

            var request = new HttpRequestMessage
            {
                Method = HttpMethod.Delete
            };

            if (_etag.HasValue)
                request.Headers.IfMatch.Add(new EntityTagHeaderValue($"\"{_etag}\""));

            return request;
        }

        public override void SetResponse(BlittableJsonReaderObject response, bool fromCache)
        {
        }

        public override bool IsReadRequest => false;
    }
}