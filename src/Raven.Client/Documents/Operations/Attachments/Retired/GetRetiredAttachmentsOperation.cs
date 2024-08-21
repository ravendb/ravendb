using System.Collections.Generic;
using Raven.Client.Documents.Attachments;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.Attachments.Retired
{
    public sealed class GetRetiredAttachmentsOperation : IOperation<IEnumerator<AttachmentEnumeratorResult>>
    {
        private readonly IEnumerable<AttachmentRequest> _attachments;

        public GetRetiredAttachmentsOperation(IEnumerable<AttachmentRequest> attachments)
        {
            _attachments = attachments;
        }

        public RavenCommand<IEnumerator<AttachmentEnumeratorResult>> GetCommand(IDocumentStore store, DocumentConventions conventions, JsonOperationContext context, HttpCache cache)
        {
            return new GetRetiredAttachmentsCommand(conventions, context, _attachments);
        }
        internal sealed class GetRetiredAttachmentsCommand : GetAttachmentsOperation.GetAttachmentsCommand
        {
            public GetRetiredAttachmentsCommand(DocumentConventions conventions, JsonOperationContext context, IEnumerable<AttachmentRequest> attachments) 
                : base(conventions, context, attachments, AttachmentType.Document)
            {
            }

            protected override string GetUrl(ServerNode node)
            {
                return $"{node.Url}/databases/{node.Database}/attachments/retire/bulk";
            }
        }
    }
}
