//-----------------------------------------------------------------------
// <copyright file="IAsyncDocumentSession.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------


namespace Raven.Client.Documents.Session
{
    /// <summary>
    /// Provides an access to DocumentSession TimeSeries API.
    /// </summary>
    public partial interface IAsyncDocumentSession
    {

        IAsyncSessionDocumentTimeSeries TimeSeriesFor(string documentId);

        IAsyncSessionDocumentTimeSeries TimeSeriesFor(object entity);

    }
}
