//-----------------------------------------------------------------------
// <copyright file="AsyncDocumentSession.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using Raven.Client.Documents.Session.TimeSeries;

namespace Raven.Client.Documents.Session
{
    /// <summary>
    /// Implements Unit of Work for accessing the RavenDB server
    /// </summary>
    public partial class AsyncDocumentSession
    {
        public IAsyncSessionDocumentTimeSeries TimeSeriesFor(string documentId, string name)
        {
            return new AsyncSessionDocumentTimeSeries<TimeSeriesEntry>(this, documentId, name);
        }

        public IAsyncSessionDocumentTimeSeries TimeSeriesFor(object entity, string name)
        {
            return new AsyncSessionDocumentTimeSeries<TimeSeriesEntry>(this, entity, name);
        }

        public IAsyncSessionDocumentTypedTimeSeries<TValues> TimeSeriesFor<TValues>(string documentId) where TValues : TimeSeriesEntry
        {
            return new AsyncSessionDocumentTimeSeries<TValues>(this, documentId, typeof(TValues).Name);
        }

        public IAsyncSessionDocumentTypedTimeSeries<TValues> TimeSeriesFor<TValues>(object entity) where TValues : TimeSeriesEntry
        {
            return new AsyncSessionDocumentTimeSeries<TValues>(this, entity, typeof(TValues).Name);
        }
    }
}
