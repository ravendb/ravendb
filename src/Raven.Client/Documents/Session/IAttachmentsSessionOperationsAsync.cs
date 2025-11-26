using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.Attachments;

namespace Raven.Client.Documents.Session
{
    /// <summary>
    /// Advanced asynchronous session operations for working with attachments.
    /// </summary>
    public interface IAttachmentsSessionOperationsAsync : IAttachmentsSessionOperationsBase
    {
        /// <summary>
        /// Checks whether an attachment exists for the specified document ID and attachment name.
        /// </summary>
        Task<bool> ExistsAsync(string documentId, string name, CancellationToken token = default);

        /// <summary>
        /// Returns the attachment for the specified document ID and attachment name.
        /// </summary>
        Task<AttachmentResult> GetAsync(string documentId, string name, CancellationToken token = default);

        /// <summary>
        /// Returns the attachment for the specified entity instance and attachment name.
        /// </summary>
        Task<AttachmentResult> GetAsync(object entity, string name, CancellationToken token = default);

        /// <summary>
        /// Returns a range of the attachment stream for the specified document ID and attachment name.
        /// </summary>
        Task<AttachmentResult> GetRangeAsync(string documentId, string name, long? from, long? to, CancellationToken token = default);

        /// <summary>
        /// Returns a range of the attachment stream for the specified entity instance and attachment name.
        /// </summary>
        Task<AttachmentResult> GetRangeAsync(object entity, string name, long? from, long? to, CancellationToken token = default);

        /// <summary>
        /// Returns an enumerator over multiple attachments. Each result includes the attachment stream and metadata.
        /// </summary>
        Task<IEnumerator<AttachmentEnumeratorResult>> GetAsync(IEnumerable<AttachmentRequest> attachments, CancellationToken token = default);

        /// <summary>
        /// Returns the attachment from a document revision for the specified document ID, attachment name, and change vector.
        /// </summary>
        Task<AttachmentResult> GetRevisionAsync(string documentId, string name, string changeVector, CancellationToken token = default);
    }
}