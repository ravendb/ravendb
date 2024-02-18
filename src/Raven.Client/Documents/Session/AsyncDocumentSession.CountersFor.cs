//-----------------------------------------------------------------------
// <copyright file="AsyncDocumentSession.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

namespace Raven.Client.Documents.Session
{
    public partial class AsyncDocumentSession
    {
        /// <inheritdoc cref="IAsyncDocumentSession.CountersFor(string)"/>
        public IAsyncSessionDocumentCounters CountersFor(string documentId)
        {
            return new AsyncSessionDocumentCounters(this, documentId);
        }

        /// <inheritdoc cref="IAsyncDocumentSession.CountersFor(object)"/>
        public IAsyncSessionDocumentCounters CountersFor(object entity)
        {
            return new AsyncSessionDocumentCounters(this, entity);
        }
    }
}
