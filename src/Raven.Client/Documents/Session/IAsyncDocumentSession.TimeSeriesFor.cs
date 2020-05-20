//-----------------------------------------------------------------------
// <copyright file="IAsyncDocumentSession.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------


using Raven.Client.Documents.Session.TimeSeries;

namespace Raven.Client.Documents.Session
{
    /// <summary>
    /// Provides an access to DocumentSession TimeSeries API.
    /// </summary>
    public partial interface IAsyncDocumentSession
    {
        IAsyncSessionDocumentTimeSeries TimeSeriesFor(string documentId, string name);
        IAsyncSessionDocumentTimeSeries TimeSeriesFor(object entity, string name);
        IAsyncSessionDocumentTypedTimeSeries<TValues> TimeSeriesFor<TValues>(string documentId, string name = null) where TValues : TimeSeriesEntry;
        IAsyncSessionDocumentTypedTimeSeries<TValues> TimeSeriesFor<TValues>(object entity, string name = null) where TValues : TimeSeriesEntry;
    }
}
