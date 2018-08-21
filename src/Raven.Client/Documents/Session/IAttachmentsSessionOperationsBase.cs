using System.IO;
using Raven.Client.Documents.Operations.Attachments;

namespace Raven.Client.Documents.Session
{
    public interface IAttachmentsSessionOperationsBase
    {
        /// <summary>
        /// Returns the attachments info of a document.
        /// </summary>
        AttachmentName[] GetNames(object entity);

        /// <summary>
        /// Stores attachment to be sent in the session.
        /// </summary>
        void Store(string documentId, string name, Stream stream, string contentType = null);
        
        /// <summary>
        /// Stores attachment to be sent in the session.
        /// </summary>
        void Store(object entity, string name, Stream stream, string contentType = null);

        /// <summary>
        ///     Marks the specified document's attachment for deletion. The attachment will be deleted when
        ///     <see cref="IDocumentSession.SaveChanges" /> is called.
        /// </summary>
        /// <param name="documentId">the document which holds the attachment</param>
        /// <param name="name">the attachment name</param>
        void Delete(string documentId, string name);

        /// <summary>
        ///     Marks the specified document's attachment for deletion. The attachment will be deleted when
        ///     <see cref="IDocumentSession.SaveChanges" /> is called.
        /// </summary>
        /// <param name="entity">instance of entity of the document which holds the attachment</param>
        /// <param name="name">the attachment name</param>
        void Delete(object entity, string name);

        /// <summary>
        ///     Marks the specified document's attachment for rename. The attachment will be renamed when
        ///     <see cref="IDocumentSession.SaveChanges" /> is called.
        /// </summary>
        /// <param name="entity">instance of entity of the document which holds the attachment</param>
        /// <param name="name">the attachment name</param>
        /// <param name="newName">the attachment new name</param>
        void Rename(object entity, string name, string newName);

        /// <summary>
        ///     Marks the specified document's attachment for rename. The attachment will be renamed when
        ///     <see cref="IDocumentSession.SaveChanges" /> is called.
        /// </summary>
        /// <param name="documentId">the document which holds the attachment</param>
        /// <param name="name">the attachment name</param>
        /// <param name="newName">the attachment new name</param>
        void Rename(string documentId, string name, string newName);

        /// <summary>
        /// Copies specified source document attachment to destination document. The operation will be executed when <see cref="IDocumentSession.SaveChanges" /> is called.
        /// </summary>
        /// <param name="sourceEntity">the document which holds the attachment</param>
        /// <param name="sourceName">the attachment name</param>
        /// <param name="destinationEntity">the document to which the attachment will be copied</param>
        /// <param name="destinationName">the attachment name</param>
        void Copy(object sourceEntity, string sourceName, object destinationEntity, string destinationName);

        /// <summary>
        /// Copies specified source document attachment to destination document. The operation will be executed when <see cref="IDocumentSession.SaveChanges" /> is called.
        /// </summary>
        /// <param name="sourceDocumentId">the document which holds the attachment</param>
        /// <param name="sourceName">the attachment name</param>
        /// <param name="destinationDocumentId">the document to which the attachment will be copied</param>
        /// <param name="destinationName">the attachment name</param>
        void Copy(string sourceDocumentId, string sourceName, string destinationDocumentId, string destinationName);

        /// <summary>
        /// Moves specified source document attachment to destination document. The operation will be executed when <see cref="IDocumentSession.SaveChanges" /> is called.
        /// </summary>
        /// <param name="sourceEntity">the document which holds the attachment</param>
        /// <param name="sourceName">the attachment name</param>
        /// <param name="destinationEntity">the document to which the attachment will be moved</param>
        /// <param name="destinationName">the attachment name</param>
        void Move(object sourceEntity, string sourceName, object destinationEntity, string destinationName);

        /// <summary>
        /// Moves specified source document attachment to destination document. The operation will be executed when <see cref="IDocumentSession.SaveChanges" /> is called.
        /// </summary>
        /// <param name="sourceDocumentId">the document which holds the attachment</param>
        /// <param name="sourceName">the attachment name</param>
        /// <param name="destinationDocumentId">the document to which the attachment will be moved</param>
        /// <param name="destinationName">the attachment name</param>
        void Move(string sourceDocumentId, string sourceName, string destinationDocumentId, string destinationName);
    }
}
