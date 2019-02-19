//-----------------------------------------------------------------------
// <copyright file="IAttachmentsSessionOperations.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using Raven.Client.Documents.Operations.Attachments;

namespace Raven.Client.Documents.Session
{
    /// <summary>
    ///     Attachments advanced synchronous session operations
    /// </summary>
    public interface IAttachmentsSessionOperations : IAttachmentsSessionOperationsBase
    {
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
    }
}
