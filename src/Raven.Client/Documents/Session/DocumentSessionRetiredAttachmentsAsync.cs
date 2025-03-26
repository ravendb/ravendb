using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Client.Documents.Operations.Attachments.Retired;
using static Raven.Client.Documents.Operations.Attachments.Retired.DeleteRetiredAttachmentOperation;

namespace Raven.Client.Documents.Session
{
    public sealed class DocumentSessionRetiredAttachmentsAsync : DocumentSessionRetiredAttachmentsBase, IRetiredAttachmentsSessionOperationsAsync
    {
        public DocumentSessionRetiredAttachmentsAsync(InMemoryDocumentSessionOperations session) : base(session)
        {
        }

        public async Task<bool> ExistsAsync(string documentId, string name, CancellationToken token = default)
        {
            using (Session.AsyncTaskHolder())
            {
                var operation = new HeadRetiredAttachmentOperation(documentId, name, null);
                Session.IncrementRequestCount();
                return await Session.Operations.SendAsync(operation, sessionInfo: SessionInfo, token).ConfigureAwait(false) != null;
            }
        }

        public async Task<AttachmentResult> GetAsync(string documentId, string name, CancellationToken token = default)
        {
            using (Session.AsyncTaskHolder())
            {
                var operation = new GetRetiredAttachmentOperation(documentId, name);
                Session.IncrementRequestCount();
                return await Session.Operations.SendAsync(operation, sessionInfo: SessionInfo, token).ConfigureAwait(false);
            }
        }

        public async Task<AttachmentResult> GetAsync(object entity, string name, CancellationToken token = default)
        {
            using (Session.AsyncTaskHolder())
            {
                if (Session.DocumentsByEntity.TryGetValue(entity, out DocumentInfo document) == false)
                    ThrowEntityNotInSessionOrMissingId(entity);

                var operation = new GetRetiredAttachmentOperation(document.Id, name);
                Session.IncrementRequestCount();
                return await Session.Operations.SendAsync(operation, sessionInfo: SessionInfo, token).ConfigureAwait(false);
            }
        }

        public async Task<IEnumerator<AttachmentEnumeratorResult>> GetAsync(IEnumerable<AttachmentRequest> attachments, CancellationToken token = default)
        {
            using (Session.AsyncTaskHolder())
            {
                var operation = new GetRetiredAttachmentsOperation(attachments);
                Session.IncrementRequestCount();
                return await Session.Operations.SendAsync(operation, SessionInfo, token).ConfigureAwait(false);
            }
        }

        public async Task DeleteAsync(string documentId, string name, bool storageOnly)
        {
            using (Session.AsyncTaskHolder())
            {
                var command = new DeleteRetiredAttachmentCommand(documentId, name, null, storageOnly);
                Session.IncrementRequestCount();
                await RequestExecutor.ExecuteAsync(command, Context, sessionInfo: SessionInfo).ConfigureAwait(false);
            }
        }

        public async Task DeleteAsync(object entity, string name, bool storageOnly)
        {
            using (Session.AsyncTaskHolder())
            {
                if (Session.DocumentsByEntity.TryGetValue(entity, out DocumentInfo document) == false)
                    ThrowEntityNotInSessionOrMissingId(entity);

                var command = new DeleteRetiredAttachmentCommand(document.Id, name, null, storageOnly);
                Session.IncrementRequestCount();
                await RequestExecutor.ExecuteAsync(command, Context, sessionInfo: SessionInfo).ConfigureAwait(false);
            }
        }
    }
}
