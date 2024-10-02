using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.Attachments
{
    /// <summary>
    /// Represents an operation to delete an attachment from the database.
    /// </summary>
    /// <remarks>
    /// This operation can be used to remove an existing attachment associated with a document.
    /// </remarks>
    public sealed class DeleteAttachmentOperation : IOperation
    {
        private readonly string _documentId;
        private readonly string _name;
        private readonly string _changeVector;

        /// <summary>
        /// Initializes a new instance of the <see cref="DeleteAttachmentOperation"/> class.
        /// </summary>
        /// <param name="documentId">The ID of the document from which the attachment will be deleted.</param>
        /// <param name="name">The name of the attachment to be deleted.</param>
        /// <param name="changeVector">An optional change vector of the attachment for optimistic concurrency control.</param>
        /// <remarks>
        /// Use this constructor to create an operation that deletes an attachment
        /// associated with the specified document. If the <paramref name="changeVector"/> is provided,
        /// it ensures that the attachment is only deleted if the specified version of the attachment is the current one.
        /// </remarks>
        public DeleteAttachmentOperation(string documentId, string name, string changeVector = null)
        {
            _documentId = documentId;
            _name = name;
            _changeVector = changeVector;
        }

        public RavenCommand GetCommand(IDocumentStore store, DocumentConventions conventions, JsonOperationContext context, HttpCache cache)
        {
            return new DeleteAttachmentCommand(_documentId, _name, _changeVector);
        }

        internal sealed class DeleteAttachmentCommand : RavenCommand
        {
            private readonly string _documentId;
            private readonly string _name;
            private readonly string _changeVector;

            public DeleteAttachmentCommand(string documentId, string name, string changeVector)
            {
                if (string.IsNullOrWhiteSpace(documentId))
                    throw new ArgumentNullException(nameof(documentId));
                if (string.IsNullOrWhiteSpace(name))
                    throw new ArgumentNullException(nameof(name));

                _documentId = documentId;
                _name = name;
                _changeVector = changeVector;
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/attachments?id={Uri.EscapeDataString(_documentId)}&name={Uri.EscapeDataString(_name)}";
                var request = new HttpRequestMessage
                {
                    Method = HttpMethods.Delete
                };
                AddChangeVectorIfNotNull(_changeVector, request);
                return request;
            }
        }
    }
}
