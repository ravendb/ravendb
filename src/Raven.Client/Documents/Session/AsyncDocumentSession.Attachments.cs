//-----------------------------------------------------------------------
// <copyright file="DocumentSession.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System.Threading.Tasks;
using Raven.Client.Documents.Attachments;
using Raven.Client.Documents.Operations;

namespace Raven.Client.Documents.Session
{
    /// <summary>
    /// Implements Unit of Work for accessing the RavenDB server
    /// </summary>
    public partial class AsyncDocumentSession
    {
        public async Task<bool> AttachmentExistsAsync(string documentId, string name)
        {
            var command = new HeadAttachmentCommand(documentId, name, null);
            await RequestExecutor.ExecuteAsync(command, Context, sessionInfo: SessionInfo).ConfigureAwait(false);
            return command.Result != null;
        }

        public async Task<AttachmentResult> GetAttachmentAsync(string documentId, string name)
        {
            var operation = new GetAttachmentOperation(documentId, name, AttachmentType.Document, null);
            return await DocumentStore.Operations.SendAsync(operation, sessionInfo: SessionInfo).ConfigureAwait(false);
        }

        public async Task<AttachmentResult> GetAttachmentAsync(object entity, string name)
        {
            if (DocumentsByEntity.TryGetValue(entity, out DocumentInfo document) == false)
                ThrowEntityNotInSession(entity);

            var operation = new GetAttachmentOperation(document.Id, name, AttachmentType.Document, null);
            return await DocumentStore.Operations.SendAsync(operation, sessionInfo: SessionInfo).ConfigureAwait(false);
        }

        public async Task<AttachmentResult> GetRevisionAttachmentAsync(string documentId, string name, string changeVector)
        {
            var operation = new GetAttachmentOperation(documentId, name, AttachmentType.Revision, changeVector);
            return await DocumentStore.Operations.SendAsync(operation, sessionInfo: SessionInfo).ConfigureAwait(false);
        }
    }
}
