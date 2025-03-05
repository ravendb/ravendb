using System.Collections.Generic;
using Raven.Client.Documents.Operations.Attachments;

namespace Raven.Client.Documents.Session
{
    /// <summary>
    ///     Attachments advanced synchronous session operations
    /// </summary>
    public interface IRetiredAttachmentsSessionOperations : IAttachmentsSessionOperationsBaseOfTheBase
    {
        /// <summary>
        /// Check if retired attachment exists
        /// </summary>
        /// <param name="documentId">The ID of the document associated with the retired attachment.</param>
        /// <param name="name">The name of the retired attachment.</param>
        bool Exists(string documentId, string name);

        /// <summary>
        /// Returns the retired attachment by the document id and attachment name.
        /// </summary>
        /// <param name="documentId">The ID of the document associated with the retired attachment.</param>
        /// <param name="name">The name of the retired attachment.</param>
        AttachmentResult Get(string documentId, string name);

        /// <summary>
        /// Returns the retired attachment by the document id and attachment name.
        /// </summary>
        /// <param name="entity">The entity associated with the document that holds the retired attachment.</param>
        /// <param name="name">The name of the retired attachment.</param>
        AttachmentResult Get(object entity, string name);

        /// <summary>
        /// Returns Enumerator of KeyValuePairs of retired attachment name and stream.
        /// </summary>
        /// <param name="attachments">The collection of attachment requests.</param>
        IEnumerator<AttachmentEnumeratorResult> Get(IEnumerable<AttachmentRequest> attachments);

        /// <summary>
        ///     Marks the specified document's retired attachment for deletion. The attachment will be deleted when
        ///     <see cref="IDocumentSession.SaveChanges" /> is called.
        /// </summary>
        /// <param name="documentId">the document which holds the retired attachment</param>
        /// <param name="name">the retired attachment name</param>
        /// <param name="storageOnly">indicates if the deletion is only from storage</param>
        void Delete(string documentId, string name, bool storageOnly);

        /// <summary>
        ///     Marks the specified document's retired attachment for deletion. The attachment will be deleted when
        ///     <see cref="IDocumentSession.SaveChanges" /> is called.
        /// </summary>
        /// <param name="entity">instance of entity of the document which holds the retired attachment</param>
        /// <param name="name">the retired attachment name</param>
        /// <param name="storageOnly">indicates if the deletion is only from storage</param>
        void Delete(object entity, string name, bool storageOnly);

        /* TODO: egor in the end open ticket to implement those in the future, if needed
        void Rename(object entity, string name, string newName);
        void Rename(string documentId, string name, string newName);
        void Copy(object sourceEntity, string sourceName, object destinationEntity, string destinationName);
        void Copy(string sourceDocumentId, string sourceName, string destinationDocumentId, string destinationName);
        void Move(object sourceEntity, string sourceName, object destinationEntity, string destinationName);
        void Move(string sourceDocumentId, string sourceName, string destinationDocumentId, string destinationName);
        */
    }
}
