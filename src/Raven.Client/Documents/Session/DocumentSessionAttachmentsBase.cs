//-----------------------------------------------------------------------
// <copyright file="DocumentSessionAttachmentsBase.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.IO;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Client.Json.Serialization;
using Sparrow.Json;

namespace Raven.Client.Documents.Session
{
    /// <summary>
    /// Abstract implementation for in memory session operations
    /// </summary>
    public abstract class DocumentSessionAttachmentsBase : AdvancedSessionExtensionBase
    {
        protected DocumentSessionAttachmentsBase(InMemoryDocumentSessionOperations session) : base(session)
        {
        }

        public AttachmentName[] GetNames(object entity)
        {
            if (entity == null)
                throw new ArgumentNullException(nameof(entity));

            if (entity is string)
                throw new ArgumentException($"{nameof(GetNames)} requires a tracked entity object, other types such as documentId are not valid.", nameof(entity));
            
            if (Session.DocumentsByEntity.TryGetValue(entity, out var document) == false)
                ThrowEntityNotInSession(entity);

            if (document.Metadata.TryGet(Constants.Documents.Metadata.Attachments, out BlittableJsonReaderArray attachments) == false)
                return Array.Empty<AttachmentName>();

            var results = new AttachmentName[attachments.Length];
            for (var i = 0; i < attachments.Length; i++)
            {
                var attachment = (BlittableJsonReaderObject)attachments[i];
                results[i] = JsonDeserializationClient.AttachmentName(attachment);
            }
            return results;
        }

        public void Store(string documentId, string name, Stream stream, string contentType = null)
        {
            if (string.IsNullOrWhiteSpace(documentId))
                throw new ArgumentNullException(nameof(documentId));
            if (string.IsNullOrWhiteSpace(name))
                throw new ArgumentNullException(nameof(name));

            if (DeferredCommandsDictionary.ContainsKey((documentId, CommandType.DELETE, null)))
                ThrowOtherDeferredCommandException(documentId, name, "store", "delete");

            if (DeferredCommandsDictionary.ContainsKey((documentId, CommandType.AttachmentPUT, name)))
                ThrowOtherDeferredCommandException(documentId, name, "store", "create");

            if (DeferredCommandsDictionary.ContainsKey((documentId, CommandType.AttachmentDELETE, name)))
                ThrowOtherDeferredCommandException(documentId, name, "store", "delete");

            if (DeferredCommandsDictionary.ContainsKey((documentId, CommandType.AttachmentMOVE, name)))
                ThrowOtherDeferredCommandException(documentId, name, "store", "rename");

            if (DocumentsById.TryGetValue(documentId, out DocumentInfo documentInfo) &&
                Session.DeletedEntities.Contains(documentInfo.Entity))
                ThrowDocumentAlreadyDeleted(documentId, name, "store", null, documentId);

            Defer(new PutAttachmentCommandData(documentId, name, stream, contentType, null));
        }

        public void Store(object entity, string name, Stream stream, string contentType = null)
        {
            if (Session.DocumentsByEntity.TryGetValue(entity, out var document) == false)
                ThrowEntityNotInSessionOrMissingId(entity);

            Store(document.Id, name, stream, contentType);
        }

        protected void ThrowEntityNotInSessionOrMissingId(object entity)
        {
            throw new ArgumentException($"{entity} is not associated with the session. Use documentId instead or track the entity in the session.", nameof(entity));
        }

        protected void ThrowEntityNotInSession(object entity)
        {
            throw new ArgumentException($"{entity} is not associated with the session. You need to track the entity in the session.", nameof(entity));
        }

        public void Delete(object entity, string name)
        {
            if (Session.DocumentsByEntity.TryGetValue(entity, out var document) == false)
                ThrowEntityNotInSessionOrMissingId(entity);

            Delete(document.Id, name);
        }

        public void Delete(string documentId, string name)
        {
            if (string.IsNullOrWhiteSpace(documentId))
                throw new ArgumentNullException(nameof(documentId));
            if (string.IsNullOrWhiteSpace(name))
                throw new ArgumentNullException(nameof(name));

            if (DeferredCommandsDictionary.ContainsKey((documentId, CommandType.DELETE, null)) ||
                DeferredCommandsDictionary.ContainsKey((documentId, CommandType.AttachmentDELETE, name)))
                return; // no-op

            if (DocumentsById.TryGetValue(documentId, out DocumentInfo documentInfo) &&
                Session.DeletedEntities.Contains(documentInfo.Entity))
                return; // no-op

            if (DeferredCommandsDictionary.ContainsKey((documentId, CommandType.AttachmentPUT, name)))
                ThrowOtherDeferredCommandException(documentId, name, "delete", "create");

            if (DeferredCommandsDictionary.ContainsKey((documentId, CommandType.AttachmentMOVE, name)))
                ThrowOtherDeferredCommandException(documentId, name, "delete", "rename");

            Defer(new DeleteAttachmentCommandData(documentId, name, null));
        }

        public void Rename(string documentId, string name, string newName)
        {
            Move(documentId, name, documentId, newName);
        }

        public void Rename(object entity, string name, string newName)
        {
            Move(entity, name, entity, newName);
        }

        public void Move(object sourceEntity, string sourceName, object destinationEntity, string destinationName)
        {
            if (sourceEntity == null)
                throw new ArgumentNullException(nameof(sourceEntity));

            if (destinationEntity == null)
                throw new ArgumentNullException(nameof(destinationEntity));

            if (Session.DocumentsByEntity.TryGetValue(sourceEntity, out DocumentInfo sourceDocument) == false)
                ThrowEntityNotInSessionOrMissingId(sourceEntity);

            if (Session.DocumentsByEntity.TryGetValue(destinationEntity, out DocumentInfo destinationDocument) == false)
                ThrowEntityNotInSessionOrMissingId(destinationEntity);

            Move(sourceDocument.Id, sourceName, destinationDocument.Id, destinationName);
        }

        public void Move(string sourceDocumentId, string sourceName, string destinationDocumentId, string destinationName)
        {
            if (string.IsNullOrWhiteSpace(sourceDocumentId))
                throw new ArgumentNullException(nameof(sourceDocumentId));
            if (string.IsNullOrWhiteSpace(sourceName))
                throw new ArgumentNullException(nameof(sourceName));
            if (string.IsNullOrWhiteSpace(destinationDocumentId))
                throw new ArgumentNullException(nameof(destinationDocumentId));
            if (string.IsNullOrWhiteSpace(destinationName))
                throw new ArgumentNullException(nameof(destinationName));

            if (string.Equals(sourceDocumentId, destinationDocumentId, StringComparison.OrdinalIgnoreCase) && sourceName == destinationName)
                return; // no-op

            if (DocumentsById.TryGetValue(sourceDocumentId, out DocumentInfo sourceDocument) && Session.DeletedEntities.Contains(sourceDocument.Entity))
                ThrowDocumentAlreadyDeleted(sourceDocumentId, sourceName, "move", destinationDocumentId, sourceDocumentId);

            if (DocumentsById.TryGetValue(destinationDocumentId, out DocumentInfo destinationDocument) && Session.DeletedEntities.Contains(destinationDocument.Entity))
                ThrowDocumentAlreadyDeleted(sourceDocumentId, sourceName, "move", destinationDocumentId, destinationDocumentId);

            if (DeferredCommandsDictionary.ContainsKey((sourceDocumentId, CommandType.AttachmentDELETE, sourceName)))
                ThrowOtherDeferredCommandException(sourceDocumentId, sourceName, "rename", "delete");

            if (DeferredCommandsDictionary.ContainsKey((sourceDocumentId, CommandType.AttachmentMOVE, sourceName)))
                ThrowOtherDeferredCommandException(sourceDocumentId, sourceName, "rename", "rename");

            if (DeferredCommandsDictionary.ContainsKey((destinationDocumentId, CommandType.AttachmentDELETE, destinationName)))
                ThrowOtherDeferredCommandException(sourceDocumentId, destinationName, "rename", "delete");

            if (DeferredCommandsDictionary.ContainsKey((destinationDocumentId, CommandType.AttachmentMOVE, destinationName)))
                ThrowOtherDeferredCommandException(sourceDocumentId, destinationName, "rename", "rename");

            Defer(new MoveAttachmentCommandData(sourceDocumentId, sourceName, destinationDocumentId, destinationName, null));
        }

        public void Copy(object sourceEntity, string sourceName, object destinationEntity, string destinationName)
        {
            if (sourceEntity == null)
                throw new ArgumentNullException(nameof(sourceEntity));
            if (destinationEntity == null)
                throw new ArgumentNullException(nameof(destinationEntity));

            if (Session.DocumentsByEntity.TryGetValue(sourceEntity, out DocumentInfo sourceDocument) == false)
                ThrowEntityNotInSessionOrMissingId(sourceEntity);

            if (Session.DocumentsByEntity.TryGetValue(destinationEntity, out DocumentInfo destinationDocument) == false)
                ThrowEntityNotInSessionOrMissingId(destinationEntity);

            Copy(sourceDocument.Id, sourceName, destinationDocument.Id, destinationName);
        }

        public void Copy(string sourceDocumentId, string sourceName, string destinationDocumentId, string destinationName)
        {
            if (string.IsNullOrWhiteSpace(sourceDocumentId))
                throw new ArgumentNullException(nameof(sourceDocumentId));
            if (string.IsNullOrWhiteSpace(sourceName))
                throw new ArgumentNullException(nameof(sourceName));
            if (string.IsNullOrWhiteSpace(destinationDocumentId))
                throw new ArgumentNullException(nameof(destinationDocumentId));
            if (string.IsNullOrWhiteSpace(destinationName))
                throw new ArgumentNullException(nameof(destinationName));

            if (string.Equals(sourceDocumentId, destinationDocumentId, StringComparison.OrdinalIgnoreCase)
                && string.Equals(sourceName, destinationName))
                return; // no-op

            if (DocumentsById.TryGetValue(sourceDocumentId, out DocumentInfo sourceDocument) && Session.DeletedEntities.Contains(sourceDocument.Entity))
                ThrowDocumentAlreadyDeleted(sourceDocumentId, sourceName, "copy", destinationDocumentId, sourceDocumentId);

            if (DocumentsById.TryGetValue(destinationDocumentId, out DocumentInfo destinationDocument) && Session.DeletedEntities.Contains(destinationDocument.Entity))
                ThrowDocumentAlreadyDeleted(sourceDocumentId, sourceName, "copy", destinationDocumentId, destinationDocumentId);

            if (DeferredCommandsDictionary.ContainsKey((sourceDocumentId, CommandType.AttachmentDELETE, sourceName)))
                ThrowOtherDeferredCommandException(sourceDocumentId, sourceName, "copy", "delete");

            if (DeferredCommandsDictionary.ContainsKey((sourceDocumentId, CommandType.AttachmentMOVE, sourceName)))
                ThrowOtherDeferredCommandException(sourceDocumentId, sourceName, "copy", "rename");

            if (DeferredCommandsDictionary.ContainsKey((destinationDocumentId, CommandType.AttachmentDELETE, destinationName)))
                ThrowOtherDeferredCommandException(destinationDocumentId, destinationName, "copy", "delete");

            if (DeferredCommandsDictionary.ContainsKey((destinationDocumentId, CommandType.AttachmentMOVE, destinationName)))
                ThrowOtherDeferredCommandException(destinationDocumentId, destinationName, "copy", "rename");

            Defer(new CopyAttachmentCommandData(sourceDocumentId, sourceName, destinationDocumentId, destinationName, null));
        }

        private static void ThrowDocumentAlreadyDeleted(string documentId, string name, string operation, string destinationDocumentId, string deletedDocumentId)
        {
            throw new InvalidOperationException($"Can't {operation} attachment '{name}' of document '{documentId}'{(destinationDocumentId != null ? $" to '{destinationDocumentId}'" : string.Empty)}', the document '{deletedDocumentId}' was already deleted in this session.");
        }

        private static void ThrowOtherDeferredCommandException(string documentId, string name, string operation, string previousOperation)
        {
            throw new InvalidOperationException($"Can't {operation} attachment '{name}' of document '{documentId}', there is a deferred command registered to {previousOperation} an attachment with '{name}' name.");
        }
    }
}
