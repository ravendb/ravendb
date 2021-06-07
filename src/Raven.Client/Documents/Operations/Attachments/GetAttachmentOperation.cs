using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;
using Raven.Client.Documents.Attachments;
using Raven.Client.Documents.Conventions;
using Raven.Client.Extensions;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.Attachments
{
    public class GetAttachmentOperation : IOperation<AttachmentResult>
    {
        private readonly string _documentId;
        private readonly string _name;
        private readonly AttachmentType _type;
        private readonly string _changeVector;

        public GetAttachmentOperation(string documentId, string name, AttachmentType type, string changeVector)
        {
            _documentId = documentId;
            _name = name;
            _type = type;
            _changeVector = changeVector;
        }

        public RavenCommand<AttachmentResult> GetCommand(IDocumentStore store, DocumentConventions conventions, JsonOperationContext context, HttpCache cache)
        {
            return new GetAttachmentCommand(context, _documentId, _name, _type, _changeVector);
        }

        private class GetAttachmentCommand : RavenCommand<AttachmentResult>
        {
            private readonly JsonOperationContext _context;
            private readonly string _documentId;
            private readonly string _name;
            private readonly AttachmentType _type;
            private readonly string _changeVector;

            public GetAttachmentCommand(JsonOperationContext context, string documentId, string name, AttachmentType type, string changeVector)
            {
                if (string.IsNullOrWhiteSpace(documentId))
                    throw new ArgumentNullException(nameof(documentId));
                if (string.IsNullOrWhiteSpace(name))
                    throw new ArgumentNullException(nameof(name));

                if (type != AttachmentType.Document && changeVector == null)
                    throw new ArgumentNullException(nameof(changeVector), $"Change Vector cannot be null for attachment type {type}");

                _context = context;
                _documentId = documentId;
                _name = name;
                _type = type;
                _changeVector = changeVector;

                ResponseType = RavenCommandResponseType.Empty;
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/attachments?id={Uri.EscapeDataString(_documentId)}&name={Uri.EscapeDataString(_name)}";
                var request = new HttpRequestMessage
                {
                    Method = HttpMethods.Get
                };

                if (_type != AttachmentType.Document)
                {
                    request.Method = HttpMethod.Post;

                    request.Content = new BlittableJsonContent(async stream =>
                    {
                        await using (var writer = new AsyncBlittableJsonTextWriter(_context, stream))
                        {
                            writer.WriteStartObject();

                            writer.WritePropertyName("Type");
                            writer.WriteString(_type.ToString());
                            writer.WriteComma();

                            writer.WritePropertyName("ChangeVector");
                            writer.WriteString(_changeVector);

                            writer.WriteEndObject();
                        }
                    });
                }

                return request;
            }

            public override async Task<ResponseDisposeHandling> ProcessResponse(JsonOperationContext context, HttpCache cache, HttpResponseMessage response, string url)
            {
                var contentType = response.Content.Headers.TryGetValues("Content-Type", out IEnumerable<string> contentTypeVale) ? contentTypeVale.First() : null;
                var changeVector = response.GetEtagHeader();
                var hash = response.Headers.TryGetValues("Attachment-Hash", out IEnumerable<string> hashVal) ? hashVal.First() : null;
                long size = 0;
                if (response.Headers.TryGetValues("Attachment-Size", out IEnumerable<string> sizeVal))
                    long.TryParse(sizeVal.First(), out size);

                var attachmentDetails = new AttachmentDetails
                {
                    ContentType = contentType,
                    Name = _name,
                    Hash = hash,
                    Size = size,
                    ChangeVector = changeVector,
                    DocumentId = _documentId
                };

                var responseStream = await response.Content.ReadAsStreamAsync().ConfigureAwait(false);
                var streamReader = new StreamWithTimeout(responseStream);
                var stream = new AttachmentStream(response, streamReader);

                Result = new AttachmentResult
                {
                    Stream = stream,
                    Details = attachmentDetails
                };

                return ResponseDisposeHandling.Manually;
            }

            public override bool IsReadRequest => true;
        }
    }
}
