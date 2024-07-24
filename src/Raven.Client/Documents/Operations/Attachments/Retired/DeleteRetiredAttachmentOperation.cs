using System;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.Attachments.Retired
{
    public sealed class DeleteRetiredAttachmentOperation : IOperation
    {
        private readonly string _documentId;
        private readonly string _name;
        private readonly string _changeVector;

        public DeleteRetiredAttachmentOperation(string documentId, string name, string changeVector = null)
        {
            _documentId = documentId;
            _name = name;
            _changeVector = changeVector;

            // TODO: egor maybe add some flag to not delete attachment from cloud, just delete its entry from attachments table? or use separate operation for that?
        }

        public RavenCommand GetCommand(IDocumentStore store, DocumentConventions conventions, JsonOperationContext context, HttpCache cache)
        {
            return new DeleteRetiredAttachmentCommand(_documentId, _name, _changeVector);
        }

        internal sealed class DeleteRetiredAttachmentCommand : DeleteAttachmentOperation.DeleteAttachmentCommand
        {
            public DeleteRetiredAttachmentCommand(string documentId, string name, string changeVector) : base(documentId, name, changeVector)
            {
            }

            protected override string GetUrl(ServerNode node)
            {
                return $"{node.Url}/databases/{node.Database}/attachments/retire?id={Uri.EscapeDataString(_documentId)}&name={Uri.EscapeDataString(_name)}";
            }
        }
    }
}
