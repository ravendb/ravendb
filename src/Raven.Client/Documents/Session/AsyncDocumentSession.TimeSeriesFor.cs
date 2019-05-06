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
    public partial class AsyncDocumentSession
    {
        public IAsyncSessionDocumentTimeSeries TimeSeriesFor(string documentId)
        {
            return new AsyncSessionDocumentTimeSeries(this, documentId);
        }

        public IAsyncSessionDocumentTimeSeries TimeSeriesFor(object entity)
        {
            return new AsyncSessionDocumentTimeSeries(this, entity);
        }
    }
}
