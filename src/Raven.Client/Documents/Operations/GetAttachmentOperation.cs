using System;
using System.IO;
using System.Net.Http;
using System.Net.Http.Headers;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations
{
    public class GetAttachmentOperation : IOperation<AttachmentResult>
    {
        private readonly string _documentId;
        private readonly string _name;

        public GetAttachmentOperation(string documentId, string name)
        {
            _documentId = documentId;
            _name = name;
        }

        public RavenCommand<AttachmentResult> GetCommand(DocumentConventions conventions, JsonOperationContext context, HttpCache cache)
        {
            return new GetAttachmentCommand(_documentId, _name);
        }

        private class GetAttachmentCommand : RavenCommand<AttachmentResult>
        {
            private readonly string _documentId;
            private readonly string _name;

            public GetAttachmentCommand(string documentId, string name)
            {
                if (string.IsNullOrWhiteSpace(documentId))
                    throw new ArgumentNullException(nameof(documentId));
                if (string.IsNullOrWhiteSpace(name))
                    throw new ArgumentNullException(nameof(name));

                _documentId = documentId;
                _name = name;

                ResponseType = RavenCommandResponseType.Stream;
            }

            public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/attachments?id={Uri.EscapeUriString(_documentId)}&name={Uri.EscapeUriString(_name)}";
                var request = new HttpRequestMessage
                {
                    Method = HttpMethods.Get,
                };
                return request;
            }

            public override void SetResponse(BlittableJsonReaderObject response, bool fromCache)
            {
                ThrowInvalidResponse();
            }

            public override void SetResponse(Stream stream, string contentType, long etag, bool fromCache)
            {
                Result = new AttachmentResult
                {
                    Stream = stream,
                    ContentType = contentType,
                    Etag = etag,
                    Name = _name,
                    DocumentId = _documentId,
                };
            }

            public override bool IsReadRequest => true;
        }
    }
}