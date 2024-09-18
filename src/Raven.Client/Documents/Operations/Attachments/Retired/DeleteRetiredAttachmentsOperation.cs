using System;
using System.Collections.Generic;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.Attachments.Retired
{
    public sealed class DeleteRetiredAttachmentsOperation : IOperation
    {
        private readonly IEnumerable<AttachmentRequest> _attachments;
        private readonly bool _storageOnly;
        public DeleteRetiredAttachmentsOperation(IEnumerable<AttachmentRequest> attachments, bool storageOnly = false)
        {
            _attachments = attachments;
            _storageOnly = storageOnly;
        }

        public RavenCommand GetCommand(IDocumentStore store, DocumentConventions conventions, JsonOperationContext context, HttpCache cache)
        {
            return new DeleteRetiredAttachmentsCommand(conventions, context, _attachments, _storageOnly);
        }

        internal sealed class DeleteRetiredAttachmentsCommand : RavenCommand
        {
            private readonly DocumentConventions _conventions;
            private readonly JsonOperationContext _context;
            internal IEnumerable<AttachmentRequest> Attachments { get; }
            internal List<AttachmentDetails> AttachmentsMetadata { get; } = new List<AttachmentDetails>();
            private readonly bool _storageOnly;

            public DeleteRetiredAttachmentsCommand(DocumentConventions conventions, JsonOperationContext context, IEnumerable<AttachmentRequest> attachments, bool storageOnly)
            {
                _conventions = conventions ?? throw new ArgumentNullException(nameof(conventions));
                _context = context;
                Attachments = attachments;
                ResponseType = RavenCommandResponseType.Empty;
                _storageOnly = storageOnly;
            }

            public string GetUrl(ServerNode node)
            {
                var url = $"{node.Url}/databases/{node.Database}/attachments/retire/bulk";
                if (_storageOnly)
                {
                    url += "&storageOnly=true";
                }

                return url;
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = GetUrl(node);

                var request = new HttpRequestMessage
                {
                    Method = HttpMethods.Delete,
                    Content = new BlittableJsonContent(async stream =>
                    {
                        await using (var writer = new AsyncBlittableJsonTextWriter(_context, stream))
                        {
                            writer.WriteStartObject();

                            writer.WritePropertyName(nameof(Attachments));

                            writer.WriteStartArray();
                            var first = true;
                            foreach (var attachment in Attachments)
                            {
                                if (first == false)
                                    writer.WriteComma();
                                first = false;

                                writer.WriteStartObject();
                                writer.WritePropertyName(nameof(AttachmentRequest.DocumentId));
                                writer.WriteString(attachment.DocumentId);
                                writer.WriteComma();
                                writer.WritePropertyName(nameof(AttachmentRequest.Name));
                                writer.WriteString(attachment.Name);
                                writer.WriteEndObject();
                            }
                            writer.WriteEndArray();

                            writer.WriteEndObject();
                        }
                    }, _conventions)
                };

                return request;
            }

            public override bool IsReadRequest => false;
        }

    }
}
