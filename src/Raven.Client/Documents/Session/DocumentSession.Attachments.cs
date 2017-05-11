//-----------------------------------------------------------------------
// <copyright file="DocumentSession.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.IO;
using System.Linq;
using Raven.Client.Documents.Attachments;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Replication.Messages;

namespace Raven.Client.Documents.Session
{
    /// <summary>
    /// Implements Unit of Work for accessing the RavenDB server
    /// </summary>
    public partial class DocumentSession
    {
        public AttachmentResult GetAttachment(string documentId, string name, Action<AttachmentResult, Stream> stream)
        {
            var operation = new GetAttachmentOperation(documentId, name, stream, AttachmentType.Document, null);
            return DocumentStore.Operations.Send(operation);
        }

        public AttachmentResult GetRevisionAttachment(string documentId, string name, ChangeVectorEntry[] changeVector, Action<AttachmentResult, Stream> stream)
        {
            var operation = new GetAttachmentOperation(documentId, name, stream, AttachmentType.Revision, changeVector);
            return DocumentStore.Operations.Send(operation);
        }

        public void StoreAttachment(string documentId, string name, Stream stream, string contentType = null)
        {
            if (string.IsNullOrWhiteSpace(documentId))
                throw new ArgumentNullException(nameof(documentId));
            if (string.IsNullOrWhiteSpace(name))
                throw new ArgumentNullException(nameof(name));

            if (_deferedCommands.OfType<DeleteCommandData>().Any(c => c.Key == documentId))
                throw new InvalidOperationException($"Can't store attachment {name} on document {documentId}, there is a deferred command registered for this document to be deleted.");

            if (_deferedCommands.OfType<PutAttachmentCommandData>().Any(c => c.Key == documentId && c.Name == name))
                throw new InvalidOperationException($"Can't store attachment {name} on document {documentId}, there is a deferred command registered for this document to be deleted.");

            if (DocumentsById.TryGetValue(documentId, out DocumentInfo documentInfo) &&
                DeletedEntities.Contains(documentInfo.Entity))
                throw new InvalidOperationException($"Can't store attachment {name} on document {documentId}, the document was already deleted in this session.");

            Defer(new PutAttachmentCommandData(documentId, name, stream, contentType, null));
        }

        public void StoreAttachment(object entity, string name, Stream stream, string contentType = null)
        {
            if (DocumentsByEntity.TryGetValue(entity, out DocumentInfo document) == false)
                throw new ArgumentException(entity + " is not associated with the session, cannot add attachment to it. " +
                                            "Use documentId instead or track the entity in the session.", nameof(entity));

            StoreAttachment(document.Id, name, stream, contentType);
        }
    }
}