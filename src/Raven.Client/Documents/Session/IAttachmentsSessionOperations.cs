using System.Collections.Generic;
using Raven.Client.Documents.Operations.Attachments;

namespace Raven.Client.Documents.Session
{
    /// <summary>
    /// Advanced synchronous session operations for working with attachments.
    /// </summary>
    public interface IAttachmentsSessionOperations : IAttachmentsSessionOperationsBase
    {
        /// <summary>
        /// Checks whether an attachment exists for the specified document ID and attachment name.
        /// </summary>
        bool Exists(string documentId, string name);

        /// <summary>
        /// Returns the attachment for the specified document ID and attachment name.
        /// </summary>
        AttachmentResult Get(string documentId, string name);

        /// <summary>
        /// Returns the attachment for the specified entity instance and attachment name.
        /// </summary>
        AttachmentResult Get(object entity, string name);

        /// <summary>
        /// Returns a range of the attachment stream for the specified document ID and attachment name.
        /// </summary>
        AttachmentResult GetRange(string documentId, string name, long? from, long? to);

        /// <summary>
        /// Returns a range of the attachment stream for the specified entity instance and attachment name.
        /// </summary>
        AttachmentResult GetRange(object entity, string name, long? from, long? to);

        /// <summary>
        /// Returns an enumerator over multiple attachments. Each result includes the attachment stream and metadata.
        /// </summary>
        IEnumerator<AttachmentEnumeratorResult> Get(IEnumerable<AttachmentRequest> attachments);

        /// <summary>
        /// Returns the attachment from a document revision for the specified document ID, attachment name, and change vector.
        /// </summary>
        AttachmentResult GetRevision(string documentId, string name, string changeVector);
    }
}