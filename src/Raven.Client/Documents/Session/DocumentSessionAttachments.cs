using System.Collections.Generic;
using Raven.Client.Documents.Attachments;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Operations.Attachments;

namespace Raven.Client.Documents.Session
{
    /// <summary>
    /// Implements Unit of Work for accessing the RavenDB server
    /// </summary>
    public sealed class DocumentSessionAttachments : DocumentSessionAttachmentsBase, IAttachmentsSessionOperations
    {
        public DocumentSessionAttachments(InMemoryDocumentSessionOperations session) : base(session)
        {
        }

        /// <summary>
        /// Check if attachment exists
        /// </summary>
        /// <param name="documentId">The document identifier</param>
        /// <param name="name">The attachment name</param>
        /// <returns>true if the attachment exists; otherwise, false</returns>
        public bool Exists(string documentId, string name)
        {
            var command = new HeadAttachmentCommand(documentId, name, null);
            RequestExecutor.Execute(command, Context, sessionInfo: SessionInfo);
            return command.Result != null;
        }

        /// <summary>
        /// Returns the attachment by the document id and attachment name
        /// </summary>
        /// <param name="documentId">The document identifier</param>
        /// <param name="name">The attachment name</param>
        /// <returns>The attachment result containing the attachment data</returns>
        public AttachmentResult Get(string documentId, string name)
        {
            var operation = new GetAttachmentOperation(documentId, name, AttachmentType.Document, null);
            return Session.Operations.Send(operation, SessionInfo);
        }

        /// <summary>
        /// Returns the attachment by the entity and attachment name
        /// </summary>
        /// <param name="entity">The entity instance whose attachment to retrieve</param>
        /// <param name="name">The attachment name</param>
        /// <returns>The attachment result containing the attachment data</returns>
        public AttachmentResult Get(object entity, string name)
        {
            if (Session.DocumentsByEntity.TryGetValue(entity, out DocumentInfo document) == false)
                ThrowEntityNotInSessionOrMissingId(entity);

            var operation = new GetAttachmentOperation(document.Id, name, AttachmentType.Document, null);
            return Session.Operations.Send(operation, SessionInfo);
        }

        /// <summary>
        /// Returns a range of the attachment by the document id and attachment name
        /// </summary>
        /// <param name="documentId">The document identifier</param>
        /// <param name="name">The attachment name</param>
        /// <param name="from">The starting byte offset (inclusive)</param>
        /// <param name="to">The ending byte offset (inclusive)</param>
        /// <returns>The attachment result containing the attachment data range</returns>
        public AttachmentResult GetRange(string documentId, string name, long? from, long? to)
        {
            var operation = new GetAttachmentOperation(documentId, name, AttachmentType.Document, null, from, to);
            return Session.Operations.Send(operation, SessionInfo);
        }

        /// <summary>
        /// Returns a range of the attachment by the entity and attachment name
        /// </summary>
        /// <param name="entity">The entity instance whose attachment to retrieve</param>
        /// <param name="name">The attachment name</param>
        /// <param name="from">The starting byte offset (inclusive)</param>
        /// <param name="to">The ending byte offset (inclusive)</param>
        /// <returns>The attachment result containing the attachment data range</returns>
        public AttachmentResult GetRange(object entity, string name, long? from, long? to)
        {
            if (Session.DocumentsByEntity.TryGetValue(entity, out DocumentInfo document) == false)
                ThrowEntityNotInSessionOrMissingId(entity);

            var operation = new GetAttachmentOperation(document.Id, name, AttachmentType.Document, null, from, to);
            return Session.Operations.Send(operation, SessionInfo);
        }

        /// <summary>
        /// Returns multiple attachments by their requests
        /// </summary>
        /// <param name="attachments">Enumerable of attachment requests specifying the documents and attachment names</param>
        /// <returns>Enumerator of attachment results containing the attachment data</returns>
        public IEnumerator<AttachmentEnumeratorResult> Get(IEnumerable<AttachmentRequest> attachments)
        {
            var operation = new GetAttachmentsOperation(attachments, AttachmentType.Document);
            return Session.Operations.Send(operation, SessionInfo);
        }

        /// <summary>
        /// Returns the revision attachment by the document id, attachment name and change vector
        /// </summary>
        /// <param name="documentId">The document identifier</param>
        /// <param name="name">The attachment name</param>
        /// <param name="changeVector">The change vector of the revision</param>
        /// <returns>The attachment result containing the attachment data from the specified revision</returns>
        public AttachmentResult GetRevision(string documentId, string name, string changeVector)
        {
            var operation = new GetAttachmentOperation(documentId, name, AttachmentType.Revision, changeVector);
            return Session.Operations.Send(operation, SessionInfo);
        }
    }
}