using System;
using System.IO;
using System.Net.Http;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json.Serialization;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.Attachments
{
    /// <summary>
    /// Represents an operation to upload an attachment to a document in the database.
    /// </summary>
    /// <remarks>
    /// This class implements the <see cref="IOperation{AttachmentDetails}"/> interface,
    /// enabling the addition of new attachments or updating existing ones for specified documents.
    /// </remarks>
    public sealed class PutAttachmentOperation : IOperation<AttachmentDetails>
    {
        private readonly string _documentId;
        private readonly string _name;
        private readonly Stream _stream;
        private readonly string _contentType;
        private readonly string _changeVector;

        /// <summary>
        /// Initializes a new instance of the <see cref="PutAttachmentOperation"/> class.
        /// </summary>
        /// <param name="documentId">The ID of the document to which the attachment will be added.</param>
        /// <param name="name">The name of the attachment.</param>
        /// <param name="stream">The stream containing the binary content of the attachment.</param>
        /// <param name="contentType">The MIME type of the attachment (optional).</param>
        /// <param name="changeVector">An optional change vector for concurrency control.</param>
        public PutAttachmentOperation(string documentId, string name, Stream stream, string contentType = null, string changeVector = null)
        {
            _documentId = documentId;
            _name = name;
            _stream = stream;
            _contentType = contentType;
            _changeVector = changeVector;
        }

        public RavenCommand<AttachmentDetails> GetCommand(IDocumentStore store, DocumentConventions conventions, JsonOperationContext context, HttpCache cache)
        {
            return new PutAttachmentCommand(_documentId, _name, _stream, _contentType, _changeVector);
        }

        internal sealed class PutAttachmentCommand : RavenCommand<AttachmentDetails>
        {
            private readonly string _documentId;
            private readonly string _name;
            private readonly Stream _stream;
            private readonly string _contentType;
            private readonly string _changeVector;
            private readonly bool _validateStream;

            public PutAttachmentCommand(string documentId, string name, Stream stream, string contentType, string changeVector) : 
                this(documentId, name, stream, contentType, changeVector, validateStream: true)
            {
            }

            internal PutAttachmentCommand(string documentId, string name, Stream stream, string contentType, string changeVector, bool validateStream)
            {
                if (string.IsNullOrWhiteSpace(documentId))
                    throw new ArgumentNullException(nameof(documentId));
                if (string.IsNullOrWhiteSpace(name))
                    throw new ArgumentNullException(nameof(name));

                _documentId = documentId;
                _name = name;
                _stream = stream;
                _contentType = contentType;
                _changeVector = changeVector;
                _validateStream = validateStream;

                if (_validateStream)
                    PutAttachmentCommandHelper.ValidateStream(stream);
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                if (_validateStream)
                    PutAttachmentCommandHelper.PrepareStream(_stream);

                url = $"{node.Url}/databases/{node.Database}/attachments?id={Uri.EscapeDataString(_documentId)}&name={Uri.EscapeDataString(_name)}";
                if (string.IsNullOrWhiteSpace(_contentType) == false)
                    url += $"&contentType={Uri.EscapeDataString(_contentType)}";
                var request = new HttpRequestMessage
                {
                    Method = HttpMethods.Put,
                    Content = new AttachmentStreamContent(_stream, CancellationToken)
                };

                AddChangeVectorIfNotNull(_changeVector, request);

                return request;
            }

            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
            {
                Result = JsonDeserializationClient.AttachmentDetails(response);
            }

            public override bool IsReadRequest => false;
        }
    }
}
