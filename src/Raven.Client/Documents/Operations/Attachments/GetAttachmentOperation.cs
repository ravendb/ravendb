using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading.Tasks;
using Raven.Client.Documents.Attachments;
using Raven.Client.Documents.Conventions;
using Raven.Client.Exceptions.Documents.Attachments;
using Raven.Client.Extensions;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.Attachments
{
    /// <summary>
    /// Represents an operation to retrieve an attachment from the database.
    /// </summary>
    public sealed class GetAttachmentOperation : IOperation<AttachmentResult>
    {
        private readonly string _documentId;
        private readonly string _name;
        private readonly AttachmentType _type;
        private readonly string _changeVector;
        private readonly long? _from;
        private readonly long? _to;

        /// <summary>
        /// Initializes a new instance of the <see cref="GetAttachmentOperation"/> class.
        /// </summary>
        /// <param name="documentId">The ID of the document associated with the attachment.</param>
        /// <param name="name">The name of the attachment to be retrieved.</param>
        /// <param name="type">The type of the attachment.</param>
        /// <param name="changeVector">change vector for optimistic concurrency control. If no concurrency control require this should be set to null</param>
        /// <param name="from">the position at which to start sending data</param>
        /// <param name="to">the position at which to stop sending data</param>
        /// <remarks>
        /// This constructor sets up an operation to retrieve a specific attachment.
        /// If the <paramref name="changeVector"/> is provided, it ensures that the operation 
        /// corresponds to the specified version of the document.
        /// </remarks>
        public GetAttachmentOperation(string documentId, string name, AttachmentType type, string changeVector, long? from = null, long? to = null)
        {
            _documentId = documentId;
            _name = name;
            _type = type;
            _changeVector = changeVector;
            _from = from;
            _to = to;
        }

        public RavenCommand<AttachmentResult> GetCommand(IDocumentStore store, DocumentConventions conventions, JsonOperationContext context, HttpCache cache)
        {
            return new GetAttachmentCommand(conventions, context, _documentId, _name, _type, _changeVector, _from, _to);
        }

        internal sealed class GetAttachmentCommand : RavenCommand<AttachmentResult>
        {
            private readonly DocumentConventions _conventions;
            private readonly JsonOperationContext _context;
            private readonly string _documentId;
            private readonly string _name;
            private readonly AttachmentType _type;
            private readonly string _changeVector;
            private readonly long? _from;
            private readonly long? _to;

            public GetAttachmentCommand(DocumentConventions conventions, JsonOperationContext context, string documentId, string name, AttachmentType type, string changeVector, long? from = null, long? to = null)
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
                _from = from;
                _to = to;

                ResponseType = RavenCommandResponseType.Empty;
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/attachments?id={Uri.EscapeDataString(_documentId)}&name={Uri.EscapeDataString(_name)}";
                var request = new HttpRequestMessage
                {
                    Method = HttpMethods.Get
                };

                if (_from is not null || _to is not null)
                {
                    request.Headers.Range = new RangeHeaderValue(_from, _to);
                }

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
                var contentType = response.Content.Headers.TryGetValues(Constants.Headers.ContentType, out IEnumerable<string> contentTypeVale) ? contentTypeVale.First() : null;
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

                var responseStream = await response.Content.ReadAsStreamWithZstdSupportAsync().ConfigureAwait(false);
                var streamReader = new StreamWithTimeout(responseStream);
                var stream = new AttachmentStream(response, streamReader);

                Result = new AttachmentResult
                {
                    Stream = stream,
                    Details = attachmentDetails
                };

                return ResponseDisposeHandling.Manually;
            }

            public override void OnResponseFailure(HttpResponseMessage response)
            {
                if (response.StatusCode == HttpStatusCode.RequestedRangeNotSatisfiable)
                {
                    InvalidAttachmentRangeException.ThrowFor(_documentId, _name, _from, _to);
                }
            }

            public override bool IsReadRequest => true;
        }
    }
}
