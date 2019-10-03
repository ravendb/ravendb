//-----------------------------------------------------------------------
// <copyright file="IAttachmentsSessionOperations.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System.Collections.Generic;
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
        /// Returns dictionary of attachments by the document id and list of attachment names.
        /// </summary>
        Dictionary<string, AttachmentResult> Get(string documentId, IEnumerable<string> names);

        /// <summary>
        /// Returns dictionary of attachments by the entity and list of attachment names.
        /// </summary>
        Dictionary<string, AttachmentResult> Get(object entity, IEnumerable<string> names);

        /// <summary>
        /// Returns the revision attachment by the document id and attachment name.
        /// </summary>
        AttachmentResult GetRevision(string documentId, string name, string changeVector);
    }
}
