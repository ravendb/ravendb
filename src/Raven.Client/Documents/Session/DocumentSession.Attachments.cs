//-----------------------------------------------------------------------
// <copyright file="DocumentSession.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.IO;
using Raven.Client.Documents.Attachments;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Replication.Messages;

namespace Raven.Client.Documents.Session
{
    /// <summary>
    /// Implements Unit of Work for accessing the RavenDB server
    /// </summary>
    public partial class DocumentSession
    {
        public AttachmentResult GetAttachment(string documentId, string name, Action<AttachmentResult, Stream> stream)
        {
            var operation = new GetAttachmentOperation(documentId, name, stream, AttachmentType.Document, null);
            return DocumentStore.Operations.Send(operation);
        }

        public AttachmentResult GetRevisionAttachment(string documentId, string name, ChangeVectorEntry[] changeVector, Action<AttachmentResult, Stream> stream)
        {
            var operation = new GetAttachmentOperation(documentId, name, stream, AttachmentType.Revision, changeVector);
            return DocumentStore.Operations.Send(operation);
        }
    }
}