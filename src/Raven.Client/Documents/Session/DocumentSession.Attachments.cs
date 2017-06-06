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
using Raven.Client.Json.Converters;
using Sparrow.Json;

namespace Raven.Client.Documents.Session
{
    /// <summary>
    /// Implements Unit of Work for accessing the RavenDB server
    /// </summary>
    public partial class DocumentSession
    {
        public AttachmentName[] GetAttachmentNames(object entity)
        {
            if (entity == null)
                return null;

            if (DocumentsByEntity.TryGetValue(entity, out DocumentInfo document) == false)
                ThrowEntityNotInSession(entity);

            if (document.Metadata.TryGet(Constants.Documents.Metadata.Attachments, out BlittableJsonReaderArray attachments) == false)
                return null;

            var results = new AttachmentResult[attachments.Length];
            for (var i = 0; i < attachments.Length; i++)
            {
                var attachment = (BlittableJsonReaderObject)attachments[i];
                results[i] = JsonDeserializationClient.AttachmentResult(attachment);
            }
            return results;
        }

        public AttachmentResultWithStream GetAttachment(string documentId, string name)
        {
            var operation = new GetAttachmentOperation(documentId, name, AttachmentType.Document, null);
            return DocumentStore.Operations.Send(operation);
        }

        public AttachmentResultWithStream GetAttachment(object entity, string name)
        {
            if (DocumentsByEntity.TryGetValue(entity, out DocumentInfo document) == false)
                ThrowEntityNotInSession(entity);

            var operation = new GetAttachmentOperation(document.Id, name, AttachmentType.Document, null);
            return DocumentStore.Operations.Send(operation);
        }

        public AttachmentResultWithStream GetRevisionAttachment(string documentId, string name, ChangeVectorEntry[] changeVector)
        {
            var operation = new GetAttachmentOperation(documentId, name, AttachmentType.Revision, changeVector);
            return DocumentStore.Operations.Send(operation);
        }

        public void StoreAttachment(string documentId, string name, Stream stream, string contentType = null)
        {
            if (string.IsNullOrWhiteSpace(documentId))
                throw new ArgumentNullException(nameof(documentId));
            if (string.IsNullOrWhiteSpace(name))
                throw new ArgumentNullException(nameof(name));

            if (_deferredCommands.OfType<DeleteCommandData>().Any(c => c.Id == documentId))
                throw new InvalidOperationException($"Can't store attachment {name} of document {documentId}, there is a deferred command registered for this document to be deleted.");

            if (_deferredCommands.OfType<PutAttachmentCommandData>().Any(c => c.Id == documentId && c.Name == name))
                throw new InvalidOperationException($"Can't store attachment {name} of document {documentId}, there is a deferred command registered to create an attachment with the same name.");

            if (_deferredCommands.OfType<DeleteAttachmentCommandData>().Any(c => c.Id == documentId && c.Name == name))
                throw new InvalidOperationException($"Can't store attachment {name} of document {documentId}, there is a deferred command registered to delete an attachment with the same name.");

            if (DocumentsById.TryGetValue(documentId, out DocumentInfo documentInfo) &&
                DeletedEntities.Contains(documentInfo.Entity))
                throw new InvalidOperationException($"Can't store attachment {name} of document {documentId}, the document was already deleted in this session.");

            Defer(new PutAttachmentCommandData(documentId, name, stream, contentType, null));
        }

        public void StoreAttachment(object entity, string name, Stream stream, string contentType = null)
        {
            if (DocumentsByEntity.TryGetValue(entity, out DocumentInfo document) == false)
                ThrowEntityNotInSession(entity);

            StoreAttachment(document.Id, name, stream, contentType);
        }

        private void ThrowEntityNotInSession(object entity)
        {
            throw new ArgumentException(entity + " is not associated with the session, cannot add attachment to it. " +
                                        "Use documentId instead or track the entity in the session.", nameof(entity));
        }

        public void DeleteAttachment(object entity, string name)
        {
            if (DocumentsByEntity.TryGetValue(entity, out DocumentInfo document) == false)
                ThrowEntityNotInSession(entity);

            DeleteAttachment(document.Id, name);
        }

        public void DeleteAttachment(string documentId, string name)
        {
            if (string.IsNullOrWhiteSpace(documentId))
                throw new ArgumentNullException(nameof(documentId));
            if (string.IsNullOrWhiteSpace(name))
                throw new ArgumentNullException(nameof(name));

            if (_deferredCommands.OfType<DeleteCommandData>().Any(c => c.Id == documentId) ||
                _deferredCommands.OfType<DeleteAttachmentCommandData>().Any(c => c.Id == documentId && c.Name == name))
                return; // no-op

            if (DocumentsById.TryGetValue(documentId, out DocumentInfo documentInfo) &&
                DeletedEntities.Contains(documentInfo.Entity))
                return; // no-op

            if (_deferredCommands.OfType<PutAttachmentCommandData>().Any(c => c.Id == documentId && c.Name == name))
                throw new InvalidOperationException($"Can't delete attachment {name} of document {documentId}, there is a deferred command registered to create an attachment with the same name.");

            Defer(new DeleteAttachmentCommandData(documentId, name, null));
        }
    }
}