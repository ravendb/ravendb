//-----------------------------------------------------------------------
// <copyright file="DocumentSessionAttachments.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using Raven.Client.Documents.Attachments;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Operations.Attachments;

namespace Raven.Client.Documents.Session
{
    /// <summary>
    /// Implements Unit of Work for accessing the RavenDB server
    /// </summary>
    public class DocumentSessionAttachments : DocumentSessionAttachmentsBase, IAttachmentsSessionOperations
    {
        public DocumentSessionAttachments(InMemoryDocumentSessionOperations session) : base(session)
        {
        }

        public bool Exists(string documentId, string name)
        {
            var command = new HeadAttachmentCommand(documentId, name, null);
            RequestExecutor.Execute(command, Context, sessionInfo: SessionInfo);
            return command.Result != null;
        }

        public AttachmentResult Get(string documentId, string name)
        {
            var operation = new GetAttachmentOperation(documentId, name, AttachmentType.Document, null);
            return Session.Operations.Send(operation, SessionInfo);
        }

        public AttachmentResult Get(object entity, string name)
        {
            if (DocumentsByEntity.TryGetValue(entity, out DocumentInfo document) == false)
                ThrowEntityNotInSession(entity);

            var operation = new GetAttachmentOperation(document.Id, name, AttachmentType.Document, null);
            return Session.Operations.Send(operation, SessionInfo);
        }

        public AttachmentResult GetRevision(string documentId, string name, string changeVector)
        {
            var operation = new GetAttachmentOperation(documentId, name, AttachmentType.Revision, changeVector);
            return Session.Operations.Send(operation, SessionInfo);
        }
    }
}
