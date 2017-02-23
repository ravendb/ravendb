using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations
{
    public class DeleteAttachmentOperation : IOperation<object>
    {
        private readonly string _documentId;
        private readonly string _name;
        private readonly long? _etag;

        public DeleteAttachmentOperation(string documentId, string name, long? etag = null)
        {
            _documentId = documentId;
            _name = name;
            _etag = etag;
        }

        public RavenCommand<object> GetCommand(DocumentConventions conventions, JsonOperationContext context, HttpCache cache)
        {
            return new DeleteAttachmentCommand(_documentId, _name, _etag);
        }

        private class DeleteAttachmentCommand : RavenCommand<object>
        {
            private readonly string _documentId;
            private readonly string _name;
            private readonly long? _etag;

            public DeleteAttachmentCommand(string documentId, string name, long? etag)
            {
                if (string.IsNullOrWhiteSpace(documentId))
                    throw new ArgumentNullException(nameof(documentId));
                if (string.IsNullOrWhiteSpace(name))
                    throw new ArgumentNullException(nameof(name));

                _documentId = documentId;
                _name = name;
                _etag = etag;
            }

            public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/attachments?id={Uri.EscapeUriString(_documentId)}&name={Uri.EscapeUriString(_name)}";
                var request = new HttpRequestMessage
                {
                    Method = HttpMethods.Delete,
                };
                AddEtagIfNotNull(_etag, request);
                return request;
            }

            public override void SetResponse(BlittableJsonReaderObject response, bool fromCache)
            {
            }

            public override bool IsReadRequest => false;
        }
    }
}