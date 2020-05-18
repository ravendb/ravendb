//-----------------------------------------------------------------------
// <copyright file="DocumentSession.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using Raven.Client.Documents.Session.TimeSeries;

namespace Raven.Client.Documents.Session
{
    /// <summary>
    /// Provides an access to DocumentSession TimeSeries API.
    /// </summary>
    public partial class DocumentSession
    {
        public ISessionDocumentTimeSeries TimeSeriesFor(string documentId, string name)
        {
            return new SessionDocumentTimeSeries<TimeSeriesEntry>(this, documentId, name);
        }

        public ISessionDocumentTimeSeries TimeSeriesFor(object entity, string name)
        {
            return new SessionDocumentTimeSeries<TimeSeriesEntry>(this, entity, name);
        }

        public ISessionDocumentTypedTimeSeries<TValues> TimeSeriesFor<TValues>(string documentId) where TValues : TimeSeriesEntry
        {
            return new SessionDocumentTimeSeries<TValues>(this, documentId, typeof(TValues).Name);
        }

        public ISessionDocumentTypedTimeSeries<TValues> TimeSeriesFor<TValues>(object entity) where TValues : TimeSeriesEntry
        {
            return new SessionDocumentTimeSeries<TValues>(this, entity, typeof(TValues).Name);
        }
    }
}
