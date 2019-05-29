//-----------------------------------------------------------------------
// <copyright file="DocumentSession.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

namespace Raven.Client.Documents.Session
{
    /// <summary>
    /// Provides an access to DocumentSession TimeSeries API.
    /// </summary>
    public partial class DocumentSession
    {
        public ISessionDocumentTimeSeries TimeSeriesFor(string documentId)
        {
            return new SessionDocumentTimeSeries(this, documentId);
        }

        public ISessionDocumentTimeSeries TimeSeriesFor(object entity)
        {
            return new SessionDocumentTimeSeries(this, entity);
        }

    }
}
