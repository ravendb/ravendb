//-----------------------------------------------------------------------
// <copyright file="ISyncAdvancedSessionOperation.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.IO;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Replication.Messages;

namespace Raven.Client.Documents.Session
{
    /// <summary>
    ///     Advanced synchronous session operations
    /// </summary>
    public partial interface IAdvancedSessionOperation
    {
        /// <summary>
        /// Returns the attachment by the document id and attachment name.
        /// </summary>
        AttachmentResult GetAttachment(string documentId, string name, Action<AttachmentResult, Stream> stream);

        /// <summary>
        /// Returns the revision attachment by the document id and attachment name.
        /// </summary>
        AttachmentResult GetRevisionAttachment(string documentId, string name, ChangeVectorEntry[] changeVector, Action<AttachmentResult, Stream> stream);

        /// <summary>
        /// Stores attachment to be sent in the session.
        /// </summary>
        void StoreAttachment(string documentId, string name, Stream stream, string contentType = null);
        
        /// <summary>
        /// Stores attachment to be sent in the session.
        /// </summary>
        void StoreAttachment(object entity, string name, Stream stream, string contentType = null);
    }
}