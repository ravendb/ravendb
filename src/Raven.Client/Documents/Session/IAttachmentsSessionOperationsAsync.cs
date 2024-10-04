//-----------------------------------------------------------------------
// <copyright file="IAttachmentsSessionOperationsAsync.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.Attachments;

namespace Raven.Client.Documents.Session
{
    /// <summary>
    ///     Advanced async attachments session operations
    /// </summary>
    public interface IAttachmentsSessionOperationsAsync : IAttachmentsSessionOperationsBase
    {
        /// <summary>
        /// Check if attachment exists
        /// </summary>
        Task<bool> ExistsAsync(string documentId, string name, CancellationToken token = default);

        /// <summary>
        /// Returns the attachment by the document id and attachment name.
        /// </summary>
        Task<AttachmentResult> GetAsync(string documentId, string name, CancellationToken token = default);

        /// <summary>
        /// Returns the attachment by the document id and attachment name.
        /// </summary>
        Task<AttachmentResult> GetAsync(object entity, string name, CancellationToken token = default);

        /// <summary>
        /// Returns a range of the attachment by the document id and attachment name.
        /// </summary>
        Task<AttachmentResult> GetRangeAsync(string documentId, string name, long? from, long? to, CancellationToken token = default);

        /// <summary>
        /// Returns Enumerator of KeyValuePairs of attachment name and stream.
        /// </summary>
        Task<IEnumerator<AttachmentEnumeratorResult>> GetAsync(IEnumerable<AttachmentRequest> attachments, CancellationToken token = default);

        /// <summary>
        /// Returns the revision attachment by the document id and attachment name.
        /// </summary>
        Task<AttachmentResult> GetRevisionAsync(string documentId, string name, string changeVector, CancellationToken token = default);
    }
}
