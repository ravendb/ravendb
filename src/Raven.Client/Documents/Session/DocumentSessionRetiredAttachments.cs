using System;
using System.Collections.Generic;
using Raven.Client.Documents.Attachments;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Client.Documents.Operations.Attachments.Retired;
using Raven.Client.Json.Serialization;
using Sparrow.Json;
using static Raven.Client.Documents.Operations.Attachments.Retired.DeleteRetiredAttachmentOperation;

namespace Raven.Client.Documents.Session
{
    /// <summary>
    /// Implements Unit of Work for accessing the RavenDB server
    /// </summary>
    public sealed class DocumentSessionRetiredAttachments : DocumentSessionRetiredAttachmentsBase, IRetiredAttachmentsSessionOperations
    {
        public DocumentSessionRetiredAttachments(InMemoryDocumentSessionOperations session) : base(session)
        {
        }

        public bool Exists(string documentId, string name)
        {
            var operation = new HeadRetiredAttachmentOperation(documentId, name, null);
            Session.IncrementRequestCount();
            return Session.Operations.Send(operation, SessionInfo) != null;
        }

        public AttachmentResult Get(string documentId, string name)
        {
            var operation = new GetRetiredAttachmentOperation(documentId, name);
            Session.IncrementRequestCount();
            return Session.Operations.Send(operation, SessionInfo);
        }

        public AttachmentResult Get(object entity, string name)
        {
            if (Session.DocumentsByEntity.TryGetValue(entity, out DocumentInfo document) == false)
                ThrowEntityNotInSessionOrMissingId(entity);

            var operation = new GetRetiredAttachmentOperation(document.Id, name);
            Session.IncrementRequestCount();
            return Session.Operations.Send(operation, SessionInfo);
        }

        public IEnumerator<AttachmentEnumeratorResult> Get(IEnumerable<AttachmentRequest> attachments)
        {
            var operation = new GetRetiredAttachmentsOperation(attachments);
            return Session.Operations.Send(operation, SessionInfo);
        }

        public void Delete(object entity, string name, bool storageOnly)
        {
            if (Session.DocumentsByEntity.TryGetValue(entity, out DocumentInfo document) == false)
                ThrowEntityNotInSessionOrMissingId(entity);

            Delete(document.Id, name, storageOnly);
        }

        public void Delete(string documentId, string name, bool storageOnly)
        {
            if (ShouldNotContinueDelete(documentId, name))
                return; // no-op

            Defer(new DeleteAttachmentCommandData(documentId, name, storageOnly));
        }

    }
}
