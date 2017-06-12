//-----------------------------------------------------------------------
// <copyright file="DocumentSession.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents.Attachments;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Replication.Messages;
using Raven.Client.Json.Converters;
using Sparrow.Json;

namespace Raven.Client.Documents.Session
{
    /// <summary>
    /// Implements Unit of Work for accessing the RavenDB server
    /// </summary>
    public partial class AsyncDocumentSession
    {
        public async Task<AttachmentResult> GetAttachmentAsync(string documentId, string name)
        {
            var operation = new GetAttachmentOperation(documentId, name, AttachmentType.Document, null);
            return await DocumentStore.Operations.SendAsync(operation).ConfigureAwait(false);
        }

        public async Task<AttachmentResult> GetAttachmentAsync(object entity, string name)
        {
            if (DocumentsByEntity.TryGetValue(entity, out DocumentInfo document) == false)
                ThrowEntityNotInSession(entity);

            var operation = new GetAttachmentOperation(document.Id, name, AttachmentType.Document, null);
            return await DocumentStore.Operations.SendAsync(operation).ConfigureAwait(false);
        }

        public async Task<AttachmentResult> GetRevisionAttachmentAsync(string documentId, string name, ChangeVectorEntry[] changeVector)
        {
            var operation = new GetAttachmentOperation(documentId, name, AttachmentType.Revision, changeVector);
            return await DocumentStore.Operations.SendAsync(operation).ConfigureAwait(false);
        }
    }
}