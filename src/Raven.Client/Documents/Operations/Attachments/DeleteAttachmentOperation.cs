using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.Attachments
{
    public sealed class DeleteAttachmentOperation : IOperation
    {
        private readonly string _documentId;
        private readonly string _name;
        private readonly string _changeVector;

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

        internal class DeleteAttachmentCommand : RavenCommand
        {
            protected readonly string _documentId;
            protected readonly string _name;
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
                url = GetUrl(node);
                var request = new HttpRequestMessage
                {
                    Method = HttpMethods.Delete
                };
                AddChangeVectorIfNotNull(_changeVector, request);
                return request;
            }

            protected virtual string GetUrl(ServerNode node)
            {
                return $"{node.Url}/databases/{node.Database}/attachments?id={Uri.EscapeDataString(_documentId)}&name={Uri.EscapeDataString(_name)}";
            }
        }
    }
}
