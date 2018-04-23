//-----------------------------------------------------------------------
// <copyright file="DocumentSessionAttachmentsAsync.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System.Threading.Tasks;
using Raven.Client.Documents.Attachments;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Operations.Attachments;

namespace Raven.Client.Documents.Session
{
    /// <summary>
    /// Implements Unit of Work for accessing the RavenDB server
    /// </summary>
    public class DocumentSessionAttachmentsAsync : DocumentSessionAttachmentsBase, IAttachmentsSessionOperationsAsync
    {
        public DocumentSessionAttachmentsAsync(InMemoryDocumentSessionOperations session) : base(session)
        {
        }

        public async Task<bool> ExistsAsync(string documentId, string name)
        {
            var command = new HeadAttachmentCommand(documentId, name, null);
            await RequestExecutor.ExecuteAsync(command, Context, sessionInfo: SessionInfo).ConfigureAwait(false);
            return command.Result != null;
        }

        public Task<AttachmentResult> GetAsync(string documentId, string name)
        {
            var operation = new GetAttachmentOperation(documentId, name, AttachmentType.Document, null);
            return Session.Operations.SendAsync(operation, sessionInfo: SessionInfo);
        }

        public Task<AttachmentResult> GetAsync(object entity, string name)
        {
            if (DocumentsByEntity.TryGetValue(entity, out DocumentInfo document) == false)
                ThrowEntityNotInSession(entity);

            var operation = new GetAttachmentOperation(document.Id, name, AttachmentType.Document, null);
            return Session.Operations.SendAsync(operation, sessionInfo: SessionInfo);
        }

        public Task<AttachmentResult> GetRevisionAsync(string documentId, string name, string changeVector)
        {
            var operation = new GetAttachmentOperation(documentId, name, AttachmentType.Revision, changeVector);
            return Session.Operations.SendAsync(operation, sessionInfo: SessionInfo);
        }
    }
}
