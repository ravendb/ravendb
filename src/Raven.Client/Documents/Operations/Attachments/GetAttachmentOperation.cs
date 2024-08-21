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
    public sealed class GetAttachmentOperation : IOperation<AttachmentResult>
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
            return new GetAttachmentCommand(conventions, context, _documentId, _name, _type, _changeVector);
        }

        internal class GetAttachmentCommand : RavenCommand<AttachmentResult>
        {
            protected readonly string _documentId;
            protected readonly string _name;

            private readonly DocumentConventions _conventions;
            private readonly JsonOperationContext _context;
            private readonly AttachmentType _type;
            private readonly string _changeVector;

            public GetAttachmentCommand(DocumentConventions conventions, JsonOperationContext context, string documentId, string name, AttachmentType type, string changeVector)
            {
                if (string.IsNullOrWhiteSpace(documentId))
                    throw new ArgumentNullException(nameof(documentId));
                if (string.IsNullOrWhiteSpace(name))
                    throw new ArgumentNullException(nameof(name));

                if (type != AttachmentType.Document && changeVector == null)
                    throw new ArgumentNullException(nameof(changeVector), $"Change Vector cannot be null for attachment type {type}");

                _conventions = conventions ?? throw new ArgumentNullException(nameof(conventions));
                _context = context;
                _documentId = documentId;
                _name = name;
                _type = type;
                _changeVector = changeVector;

                ResponseType = RavenCommandResponseType.Empty;
            }

            protected virtual string GetUrl(ServerNode node)
            {
                return $"{node.Url}/databases/{node.Database}/attachments?id={Uri.EscapeDataString(_documentId)}&name={Uri.EscapeDataString(_name)}";
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = GetUrl(node);
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
                    }, _conventions);
                }

                return request;
            }

            public override async Task<ResponseDisposeHandling> ProcessResponse(JsonOperationContext context, HttpCache cache, HttpResponseMessage response, string url)
            {
                Result = await AttachmentResult(response, _name, _documentId).ConfigureAwait(false);
                return ResponseDisposeHandling.Manually;
            }

            public override bool IsReadRequest => true;

            internal static async Task<AttachmentResult> AttachmentResult(HttpResponseMessage response, string name, string documentId)
            {
                var contentType = response.Content.Headers.TryGetValues(Constants.Headers.ContentType, out IEnumerable<string> contentTypeVale) ? contentTypeVale.First() : null;
                var changeVector = response.GetEtagHeader();
                var hash = response.Headers.TryGetValues("Attachment-Hash", out IEnumerable<string> hashVal) ? hashVal.First() : null;
                long size = 0;
                if (response.Headers.TryGetValues("Attachment-Size", out IEnumerable<string> sizeVal))
                    long.TryParse(sizeVal.First(), out size);
                DateTime? attachmentRetireAt = null;
                if (response.Headers.TryGetValues(Constants.Headers.AttachmentRetireAt, out IEnumerable<string> dt))
                {
                    if (DateTime.TryParse(dt.First(), out var retireAt))
                    {
                        attachmentRetireAt = retireAt;
                    }
                }
                int flags = 0;
                if (response.Headers.TryGetValues(Constants.Headers.AttachmentFlags, out IEnumerable<string> flagsVal))
                    int.TryParse(flagsVal.First(), out flags);

                var attachmentDetails = new AttachmentDetails
                {
                    ContentType = contentType,
                    Name = name,
                    Hash = hash,
                    Size = size,
                    ChangeVector = changeVector,
                    DocumentId = documentId,
                    RetireAt = attachmentRetireAt,
                    Flags = (AttachmentFlags)flags
                };

                var responseStream = await response.Content.ReadAsStreamWithZstdSupportAsync().ConfigureAwait(false);
                var streamReader = new StreamWithTimeout(responseStream);
                var stream = new AttachmentStream(response, streamReader);
                var result = new AttachmentResult
                {
                    Stream = stream,
                    Details = attachmentDetails
                };
                return result;
            }
        }
    }
}
