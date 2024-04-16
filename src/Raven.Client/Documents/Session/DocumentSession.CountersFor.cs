//-----------------------------------------------------------------------
// <copyright file="DocumentSession.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

namespace Raven.Client.Documents.Session
{
    public partial class DocumentSession
    {
        /// <inheritdoc cref="IDocumentSession.CountersFor(string)"/>
        public ISessionDocumentCounters CountersFor(string documentId)
        {
            return new SessionDocumentCounters(this, documentId);
        }

        /// <inheritdoc cref="IDocumentSession.CountersFor(object)"/>
        public ISessionDocumentCounters CountersFor(object entity)
        {
            return new SessionDocumentCounters(this, entity);
        }

    }
}
