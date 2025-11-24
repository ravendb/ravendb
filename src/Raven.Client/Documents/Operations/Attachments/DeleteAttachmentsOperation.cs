using System;
using System.Collections.Generic;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.Attachments
{
    /// <summary>
    /// Represents an operation to delete multiple attachments from the database in a single request.
    /// </summary>
    /// <remarks>
    /// This operation can be used to efficiently remove multiple attachments associated with documents
    /// by sending a bulk delete request to the server.
    /// </remarks>
    public sealed class DeleteAttachmentsOperation : IOperation
    {
        private readonly IEnumerable<AttachmentRequest> _attachments;

        /// <summary>
        /// Initializes a new instance of the <see cref="DeleteAttachmentsOperation"/> class.
        /// </summary>
        /// <param name="attachments">A collection of attachment requests specifying which attachments to delete.</param>
        /// <remarks>
        /// Use this constructor to create an operation that deletes multiple attachments in a single bulk request.
        /// Each <see cref="AttachmentRequest"/> should specify the document ID and attachment name.
        /// </remarks>
        public DeleteAttachmentsOperation(IEnumerable<AttachmentRequest> attachments)
        {
            _attachments = attachments;
        }

        public RavenCommand GetCommand(IDocumentStore store, DocumentConventions conventions, JsonOperationContext context, HttpCache cache)
        {
            return new DeleteAttachmentsCommand(conventions, context, _attachments);
        }

        internal sealed class DeleteAttachmentsCommand : RavenCommand
        {
            private readonly DocumentConventions _conventions;
            private readonly JsonOperationContext _context;
            internal IEnumerable<AttachmentRequest> Attachments { get; }
            internal List<AttachmentDetails> AttachmentsMetadata { get; } = new List<AttachmentDetails>();

            public DeleteAttachmentsCommand(DocumentConventions conventions, JsonOperationContext context, IEnumerable<AttachmentRequest> attachments)
            {
                _conventions = conventions ?? throw new ArgumentNullException(nameof(conventions));
                _context = context;
                Attachments = attachments;
                ResponseType = RavenCommandResponseType.Empty;
            }

            public string GetUrl(ServerNode node)
            {
                var url = $"{node.Url}/databases/{node.Database}/attachments/bulk";

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
