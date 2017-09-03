//-----------------------------------------------------------------------
// <copyright file="DocumentSession.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using Raven.Client.Documents.Attachments;
using Raven.Client.Documents.Operations;

namespace Raven.Client.Documents.Session
{
    /// <summary>
    /// Implements Unit of Work for accessing the RavenDB server
    /// </summary>
    public partial class DocumentSession
    {
        public bool AttachmentExists(string documentId, string name)
        {
            var command = new HeadAttachmentCommand(documentId, name, null);
            RequestExecutor.Execute(command, Context, sessionInfo: SessionInfo);
            return command.Result != null;
        }

        public AttachmentResult GetAttachment(string documentId, string name)
        {
            var operation = new GetAttachmentOperation(documentId, name, AttachmentType.Document, null);
            return DocumentStore.Operations.Send(operation, SessionInfo);
        }

        public AttachmentResult GetAttachment(object entity, string name)
        {
            if (DocumentsByEntity.TryGetValue(entity, out DocumentInfo document) == false)
                ThrowEntityNotInSession(entity);

            var operation = new GetAttachmentOperation(document.Id, name, AttachmentType.Document, null);
            return DocumentStore.Operations.Send(operation, SessionInfo);
        }

        public AttachmentResult GetRevisionAttachment(string documentId, string name, string changeVector)
        {
            var operation = new GetAttachmentOperation(documentId, name, AttachmentType.Revision, changeVector);
            return DocumentStore.Operations.Send(operation, SessionInfo);
        }
    }
}
