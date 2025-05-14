//-----------------------------------------------------------------------
// <copyright file="IRetiredAttachmentsSessionOperationsAsync.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.Attachments;

namespace Raven.Client.Documents.Session
{
    /// <summary>
    ///     Advanced async retired attachments session operations
    /// </summary>
    public interface IRetiredAttachmentsSessionOperationsAsync : IAttachmentsSessionOperationsBaseOfTheBase
    {
        /// <summary>
        /// Check if retired attachment exists
        /// </summary>
        /// <param name="documentId">The ID of the document associated with the retired attachment.</param>
        /// <param name="name">The name of the retired attachment.</param>
        /// <param name="token">The cancellation token.</param>
        Task<bool> ExistsAsync(string documentId, string name, CancellationToken token = default);

        /// <summary>
        /// Returns the retired attachment by the document id and attachment name.
        /// </summary>
        /// <param name="documentId">The ID of the document associated with the retired attachment.</param>
        /// <param name="name">The name of the retired attachment.</param>
        /// <param name="token">The cancellation token.</param>
        Task<AttachmentResult> GetAsync(string documentId, string name, CancellationToken token = default);

        /// <summary>
        /// Returns the retired attachment by the document id and attachment name.
        /// </summary>
        /// <param name="entity">The entity associated with the document that holds the retired attachment.</param>
        /// <param name="name">The name of the retired attachment.</param>
        /// <param name="token">The cancellation token.</param>
        Task<AttachmentResult> GetAsync(object entity, string name, CancellationToken token = default);

        /// <summary>
        /// Returns Enumerator of KeyValuePairs of retired attachment name and stream.
        /// </summary>
        /// <param name="attachments">The collection of attachment requests.</param>
        /// <param name="token">The cancellation token.</param>
        Task<IEnumerator<AttachmentEnumeratorResult>> GetAsync(IEnumerable<AttachmentRequest> attachments, CancellationToken token = default);

        /// <summary>
        ///     Marks the specified document's retired attachment for deletion. The attachment will be deleted when
        ///     <see cref="IDocumentSession.SaveChanges" /> is called.
        /// </summary>
        /// <param name="documentId">The ID of the document which holds the retired attachment.</param>
        /// <param name="name">The name of the retired attachment.</param>
        /// <param name="storageOnly">Indicates if the deletion is only from storage.</param>
        Task DeleteAsync(string documentId, string name, bool storageOnly);

        /// <summary>
        ///     Marks the specified document's retired attachment for deletion. The attachment will be deleted when
        ///     <see cref="IDocumentSession.SaveChanges" /> is called.
        /// </summary>
        /// <param name="entity">The entity of the document which holds the retired attachment.</param>
        /// <param name="name">The name of the retired attachment.</param>
        /// <param name="storageOnly">Indicates if the deletion is only from storage.</param>
        Task DeleteAsync(object entity, string name, bool storageOnly);
    }
}
