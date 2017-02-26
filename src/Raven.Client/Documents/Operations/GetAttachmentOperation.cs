using System;
using System.IO;
using System.Net.Http;
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
        private readonly Action<AttachmentResult, Stream> _handleStreamResponse;

        public GetAttachmentOperation(string documentId, string name, Action<AttachmentResult, Stream> handleStreamResponse)
        {
            _documentId = documentId;
            _name = name;
            _handleStreamResponse = handleStreamResponse;
        }

        public RavenCommand<AttachmentResult> GetCommand(DocumentConventions conventions, JsonOperationContext context, HttpCache cache)
        {
            return new GetAttachmentCommand(_documentId, _name, _handleStreamResponse);
        }

        private class GetAttachmentCommand : RavenCommand<AttachmentResult>
        {
            private readonly string _documentId;
            private readonly string _name;
            private readonly Action<AttachmentResult, Stream> _handleStreamResponse;

            public GetAttachmentCommand(string documentId, string name, Action<AttachmentResult, Stream> handleStreamResponse)
            {
                if (string.IsNullOrWhiteSpace(documentId))
                    throw new ArgumentNullException(nameof(documentId));
                if (string.IsNullOrWhiteSpace(name))
                    throw new ArgumentNullException(nameof(name));
                if (string.IsNullOrWhiteSpace(name))
                    throw new ArgumentNullException(nameof(handleStreamResponse));

                _documentId = documentId;
                _name = name;
                _handleStreamResponse = handleStreamResponse;

                ResponseType = RavenCommandResponseType.Stream;
            }

            public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/attachments?id={Uri.EscapeDataString(_documentId)}&name={Uri.EscapeDataString(_name)}";
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
                if (stream == null)
                    return;

                Result = new AttachmentResult
                {
                    ContentType = contentType,
                    Etag = etag,
                    Name = _name,
                    DocumentId = _documentId,
                };

                _handleStreamResponse(Result, stream);
            }

            public override bool IsReadRequest => true;
        }
    }
}