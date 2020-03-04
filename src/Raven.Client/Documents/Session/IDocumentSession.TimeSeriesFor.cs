//-----------------------------------------------------------------------
// <copyright file="IDocumentSession.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------


namespace Raven.Client.Documents.Session
{
    /// <summary>
    /// Provides an access to DocumentSession TimeSeries API.
    /// </summary>
    public partial interface IDocumentSession
    {

        ISessionDocumentTimeSeries TimeSeriesFor(string documentId, string name);

        ISessionDocumentTimeSeries TimeSeriesFor(object entity, string name);

    }
}
