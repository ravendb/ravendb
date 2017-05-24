using System;
using System.IO;
using System.Net.Http;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json.Converters;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations
{
    public class PutAttachmentOperation : IOperation<AttachmentResult>
    {
        private readonly string _documentId;
        private readonly string _name;
        private readonly Stream _stream;
        private readonly string _contentType;
        private readonly long? _etag;

        public PutAttachmentOperation(string documentId, string name, Stream stream, string contentType = null, long? etag = null)
        {
            _documentId = documentId;
            _name = name;
            _stream = stream;
            _contentType = contentType;
            _etag = etag;
        }

        public RavenCommand<AttachmentResult> GetCommand(DocumentConventions conventions, JsonOperationContext context, HttpCache cache)
        {
            return new PutAttachmentCommand(_documentId, _name, _stream, _contentType, _etag);
        }

        private class PutAttachmentCommand : RavenCommand<AttachmentResult>
        {
            private readonly string _documentId;
            private readonly string _name;
            private readonly Stream _stream;
            private readonly string _contentType;
            private readonly long? _etag;

            public PutAttachmentCommand(string documentId, string name, Stream stream, string contentType, long? etag)
            {
                if (string.IsNullOrWhiteSpace(documentId))
                    throw new ArgumentNullException(nameof(documentId));
                if (string.IsNullOrWhiteSpace(name))
                    throw new ArgumentNullException(nameof(name));

                _documentId = documentId;
                _name = name;
                _stream = stream;
                _contentType = contentType;
                _etag = etag;

                if (_stream.CanRead == false)
                    PutAttachmentCommandData.ThrowNotReadableStream();
                if (_stream.CanSeek == false)
                    PutAttachmentCommandData.ThrowNotSeekableStream();
                if (_stream.Position != 0)
                    PutAttachmentCommandData.ThrowPositionNotZero(_stream.Position);
            }

            public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
            {
                _stream.Position = 0;

                url = $"{node.Url}/databases/{node.Database}/attachments?id={Uri.EscapeDataString(_documentId)}&name={Uri.EscapeDataString(_name)}";
                if (string.IsNullOrWhiteSpace(_contentType) == false)
                    url += $"&contentType={Uri.EscapeDataString(_contentType)}";
                var request = new HttpRequestMessage
                {
                    Method = HttpMethods.Put,
                    Content = new AttachmentStreamContent(_stream, CancellationToken)
                };

                AddEtagIfNotNull(_etag, request);

                return request;
            }

            public override void SetResponse(BlittableJsonReaderObject response, bool fromCache)
            {
                Result = JsonDeserializationClient.AttachmentResult(response);
            }

            public override bool IsReadRequest => false;
        }
    }
}