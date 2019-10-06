//-----------------------------------------------------------------------
// <copyright file="DocumentSessionAttachments.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System.Collections.Generic;
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
                ThrowEntityNotInSessionOrMissingId(entity);

            var operation = new GetAttachmentOperation(document.Id, name, AttachmentType.Document, null);
            return Session.Operations.Send(operation, SessionInfo);
        }

        public Dictionary<string, AttachmentResult> Get(string documentId, IEnumerable<string> names)
        {
            var operation = new GetAttachmentsOperation(documentId, names, AttachmentType.Document);
            return Session.Operations.Send(operation, SessionInfo);
        }

        public Dictionary<string, AttachmentResult> Get(object entity, IEnumerable<string> names)
        {
            if (DocumentsByEntity.TryGetValue(entity, out DocumentInfo document) == false)
                ThrowEntityNotInSessionOrMissingId(entity);

            var operation = new GetAttachmentsOperation(document.Id, names, AttachmentType.Document);
            return Session.Operations.Send(operation, SessionInfo);
        }

        public Dictionary<string, AttachmentResult> GetAll(string documentId)
        {
            var operation = new GetAttachmentsOperation(documentId, AttachmentType.Document);
            return Session.Operations.Send(operation, SessionInfo);
        }

        public Dictionary<string, AttachmentResult> GetAll(object entity)
        {
            if (DocumentsByEntity.TryGetValue(entity, out DocumentInfo document) == false)
                ThrowEntityNotInSessionOrMissingId(entity);

            var operation = new GetAttachmentsOperation(document.Id, AttachmentType.Document);
            return Session.Operations.Send(operation, SessionInfo);
        }

        public AttachmentResult GetRevision(string documentId, string name, string changeVector)
        {
            var operation = new GetAttachmentOperation(documentId, name, AttachmentType.Revision, changeVector);
            return Session.Operations.Send(operation, SessionInfo);
        }
    }
}
