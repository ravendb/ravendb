using System;
using Raven.Client.Documents.Attachments;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Sparrow.Json;
using static Raven.Client.Documents.Operations.Attachments.GetAttachmentOperation;

namespace Raven.Client.Documents.Operations.Attachments.Retired
{
    public sealed class GetRetiredAttachmentOperation : IOperation<AttachmentResult>
    {
        private readonly string _documentId;
        private readonly string _name;

        public GetRetiredAttachmentOperation(string documentId, string name)
        {
            _documentId = documentId;
            _name = name;
        }

        public RavenCommand<AttachmentResult> GetCommand(IDocumentStore store, DocumentConventions conventions, JsonOperationContext context, HttpCache cache)
        {
            return new GetRetiredAttachmentCommand(conventions, context, _documentId, _name);
        }

        internal sealed class GetRetiredAttachmentCommand : GetAttachmentCommand
        {
            public GetRetiredAttachmentCommand(DocumentConventions conventions, JsonOperationContext context, string documentId, string name)
                : base(conventions, context, documentId, name, AttachmentType.Document, null)
            {
            }
            protected override string GetUrl(ServerNode node)
            {
                return $"{node.Url}/databases/{node.Database}/attachments/retire?id={Uri.EscapeDataString(_documentId)}&name={Uri.EscapeDataString(_name)}";
            }
        }
    }
}
