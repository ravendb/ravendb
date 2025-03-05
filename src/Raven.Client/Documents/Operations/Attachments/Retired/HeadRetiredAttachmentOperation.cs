using System;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.Attachments.Retired
{
    public sealed class HeadRetiredAttachmentOperation : IOperation<string>
    {
        private readonly string _documentId;
        private readonly string _name;
        private readonly string _changeVector;

        public HeadRetiredAttachmentOperation(string documentId, string name, string changeVector)
        {
            _documentId = documentId;
            _name = name;
            _changeVector = changeVector;
        }

        public RavenCommand<string> GetCommand(IDocumentStore store, DocumentConventions conventions, JsonOperationContext context, HttpCache cache)
        {
            return new HeadRetiredAttachmentCommand(_documentId, _name, _changeVector);
        }

        internal sealed class HeadRetiredAttachmentCommand : HeadAttachmentCommand
        {
            public HeadRetiredAttachmentCommand(string documentId, string name, string changeVector)
                : base(documentId, name, changeVector)
            {
            }

            protected override string GetUrl(ServerNode node)
            {
                return $"{node.Url}/databases/{node.Database}/attachments/retire?id={Uri.EscapeDataString(_documentId)}&name={Uri.EscapeDataString(_name)}";
            }

        }
    }
}
