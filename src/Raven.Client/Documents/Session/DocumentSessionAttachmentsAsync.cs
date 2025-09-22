using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Attachments;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Operations.Attachments;

namespace Raven.Client.Documents.Session
{
    /// <summary>
    /// Implements Unit of Work for accessing the RavenDB server
    /// </summary>
    public sealed class DocumentSessionAttachmentsAsync : DocumentSessionAttachmentsBase, IAttachmentsSessionOperationsAsync
    {
        public DocumentSessionAttachmentsAsync(InMemoryDocumentSessionOperations session) : base(session)
        {
        }

        /// <summary>
        /// Check if attachment exists asynchronously
        /// </summary>
        /// <param name="documentId">The document identifier</param>
        /// <param name="name">The attachment name</param>
        /// <param name="token">The cancellation token</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains true if the attachment exists; otherwise, false</returns>
        public async Task<bool> ExistsAsync(string documentId, string name, CancellationToken token = default)
        {
            using (Session.AsyncTaskHolder())
            {
                var command = new HeadAttachmentCommand(documentId, name, null);
                Session.IncrementRequestCount();
                await RequestExecutor.ExecuteAsync(command, Context, sessionInfo: SessionInfo, token).ConfigureAwait(false);
                return command.Result != null;
            }
        }

        /// <summary>
        /// Returns the attachment by the document id and attachment name asynchronously
        /// </summary>
        /// <param name="documentId">The document identifier</param>
        /// <param name="name">The attachment name</param>
        /// <param name="token">The cancellation token</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains the attachment data</returns>
        public async Task<AttachmentResult> GetAsync(string documentId, string name, CancellationToken token = default)
        {
            using (Session.AsyncTaskHolder())
            {
                
                var operation = new GetAttachmentOperation(documentId, name, AttachmentType.Document, null);
                Session.IncrementRequestCount();
                return await Session.Operations.SendAsync(operation, sessionInfo: SessionInfo, token).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Returns the attachment by the entity and attachment name asynchronously
        /// </summary>
        /// <param name="entity">The entity instance whose attachment to retrieve</param>
        /// <param name="name">The attachment name</param>
        /// <param name="token">The cancellation token</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains the attachment data</returns>
        public async Task<AttachmentResult> GetAsync(object entity, string name, CancellationToken token = default)
        {
            using (Session.AsyncTaskHolder())
            {
                if (Session.DocumentsByEntity.TryGetValue(entity, out DocumentInfo document) == false)
                    ThrowEntityNotInSessionOrMissingId(entity);
               
                var operation = new GetAttachmentOperation(document.Id, name, AttachmentType.Document, null);
                Session.IncrementRequestCount();
                return await Session.Operations.SendAsync(operation, sessionInfo: SessionInfo, token).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Returns a range of the attachment by the document id and attachment name asynchronously
        /// </summary>
        /// <param name="documentId">The document identifier</param>
        /// <param name="name">The attachment name</param>
        /// <param name="from">The starting byte offset (inclusive)</param>
        /// <param name="to">The ending byte offset (inclusive)</param>
        /// <param name="token">The cancellation token</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains the attachment data range</returns>
        public async Task<AttachmentResult> GetRangeAsync(string documentId, string name, long? from, long? to, CancellationToken token = default)
        {
            using (Session.AsyncTaskHolder())
            {
                var operation = new GetAttachmentOperation(documentId, name, AttachmentType.Document, null, from, to);
                Session.IncrementRequestCount();
                return await Session.Operations.SendAsync(operation, sessionInfo: SessionInfo, token).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Returns a range of the attachment by the entity and attachment name asynchronously
        /// </summary>
        /// <param name="entity">The entity instance whose attachment to retrieve</param>
        /// <param name="name">The attachment name</param>
        /// <param name="from">The starting byte offset (inclusive)</param>
        /// <param name="to">The ending byte offset (inclusive)</param>
        /// <param name="token">The cancellation token</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains the attachment data range</returns>
        public async Task<AttachmentResult> GetRangeAsync(object entity, string name, long? from, long? to, CancellationToken token = default)
        {
            using (Session.AsyncTaskHolder())
            {
                if (Session.DocumentsByEntity.TryGetValue(entity, out DocumentInfo document) == false)
                    ThrowEntityNotInSessionOrMissingId(entity);

                var operation = new GetAttachmentOperation(document.Id, name, AttachmentType.Document, null, from, to);
                Session.IncrementRequestCount();
                return await Session.Operations.SendAsync(operation, sessionInfo: SessionInfo, token).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Returns multiple attachments by their requests asynchronously
        /// </summary>
        /// <param name="attachments">Enumerable of attachment requests specifying the documents and attachment names</param>
        /// <param name="token">The cancellation token</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains an enumerator of attachment results</returns>
        public Task<IEnumerator<AttachmentEnumeratorResult>> GetAsync(IEnumerable<AttachmentRequest> attachments, CancellationToken token = default)
        {
            var operation = new GetAttachmentsOperation(attachments, AttachmentType.Document);
            return Session.Operations.SendAsync(operation, SessionInfo, token);
        }

        /// <summary>
        /// Returns the revision attachment by the document id, attachment name and change vector asynchronously
        /// </summary>
        /// <param name="documentId">The document identifier</param>
        /// <param name="name">The attachment name</param>
        /// <param name="changeVector">The change vector of the revision</param>
        /// <param name="token">The cancellation token</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains the attachment data from the specified revision</returns>
        public async Task<AttachmentResult> GetRevisionAsync(string documentId, string name, string changeVector, CancellationToken token = default)
        {
            using (Session.AsyncTaskHolder())
            {
                var operation = new GetAttachmentOperation(documentId, name, AttachmentType.Revision, changeVector);
                Session.IncrementRequestCount();
                return await Session.Operations.SendAsync(operation, sessionInfo: SessionInfo, token).ConfigureAwait(false);
            }
        }
    }
}