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
        /// Returns a range of the attachment by the document id and attachment name.
        /// </summary>
        AttachmentResult GetRange(string documentId, string name, long? from, long? to);

        /// <summary>
        /// Returns a range of the attachment by the document id and attachment name.
        /// </summary>
        AttachmentResult GetRange(object entity, string name, long? from, long? to);

        /// <summary>
        /// Returns Enumerator of KeyValuePairs of attachment name and stream.
        /// </summary>
        IEnumerator<AttachmentEnumeratorResult> Get(IEnumerable<AttachmentRequest> attachments);

        /// <summary>
        /// Returns the revision attachment by the document id and attachment name.
        /// </summary>
        AttachmentResult GetRevision(string documentId, string name, string changeVector);
    }
}
