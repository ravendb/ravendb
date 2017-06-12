using System;
using System.IO;
using System.Net.Http;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Http;
using Raven.Client.Json.Converters;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations
{
    public class PutAttachmentOperation : IOperation<AttachmentDetails>
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

        public RavenCommand<AttachmentDetails> GetCommand(IDocumentStore store, JsonOperationContext context, HttpCache cache)
        {
            return new PutAttachmentCommand(_documentId, _name, _stream, _contentType, _etag);
        }

        private class PutAttachmentCommand : RavenCommand<AttachmentDetails>
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

                PutAttachmentCommandHelper.ValidateStream(stream);
            }

            public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
            {
                PutAttachmentCommandHelper.PrepareStream(_stream);

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
                Result = JsonDeserializationClient.AttachmentDetails(response);
            }

            public override bool IsReadRequest => false;
        }
    }
}