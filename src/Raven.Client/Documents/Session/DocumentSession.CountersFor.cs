//-----------------------------------------------------------------------
// <copyright file="DocumentSession.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

namespace Raven.Client.Documents.Session
{
    /// <summary>
    /// Implements Unit of Work for accessing the RavenDB server
    /// </summary>
    public partial class DocumentSession
    {
        public ISessionDocumentCounters CountersFor(string documentId)
        {
            return new SessionDocumentCounters(this, documentId);
        }

        public ISessionDocumentCounters CountersFor(object entity)
        {
            return new SessionDocumentCounters(this, entity);
        }

    }
}
