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
        private readonly bool _storageOnly;

        public DeleteRetiredAttachmentOperation(string documentId, string name, string changeVector = null, bool storageOnly = false)
        {
            _documentId = documentId;
            _name = name;
            _changeVector = changeVector;
            _storageOnly = storageOnly;
        }

        public RavenCommand GetCommand(IDocumentStore store, DocumentConventions conventions, JsonOperationContext context, HttpCache cache)
        {
            return new DeleteRetiredAttachmentCommand(_documentId, _name, _changeVector, _storageOnly);
        }

        internal sealed class DeleteRetiredAttachmentCommand : DeleteAttachmentOperation.DeleteAttachmentCommand
        {
            private readonly bool _storageOnly;

            public DeleteRetiredAttachmentCommand(string documentId, string name, string changeVector, bool storageOnly) : base(documentId, name, changeVector)
            {
                _storageOnly = storageOnly;
            }

            protected override string GetUrl(ServerNode node)
            {
                var url = $"{node.Url}/databases/{node.Database}/attachments/retire?id={Uri.EscapeDataString(_documentId)}&name={Uri.EscapeDataString(_name)}";
                if (_storageOnly)
                {
                    url += "&storageOnly=true";
                }

                return url;
            }
        }
    }
}
