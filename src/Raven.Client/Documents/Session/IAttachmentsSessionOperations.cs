//-----------------------------------------------------------------------
// <copyright file="IAttachmentsSessionOperations.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System.IO;
using Raven.Client.Documents.Operations.Attachments;

namespace Raven.Client.Documents.Session
{
    /// <summary>
    ///     Attachments advanced synchronous session operations
    /// </summary>
    public interface IAttachmentsSessionOperations
    {
        /// <summary>
        /// Returns the attachments info of a document.
        /// </summary>
        AttachmentName[] GetNames(object entity);

        /// <summary>
        /// Check if attachment exists
        /// </summary>
        bool Exists(string documentId, string name);

        /// <summary>
        /// Returns the attachment by the document id and attachment name.
        /// </summary>
        AttachmentResult Get(string documentId, string name);

        /// <summary>
        /// Returns the attachment by the document id and attachment name.
        /// </summary>
        AttachmentResult Get(object entity, string name);

        /// <summary>
        /// Returns the revision attachment by the document id and attachment name.
        /// </summary>
        AttachmentResult GetRevision(string documentId, string name, string changeVector);

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
    }
}
