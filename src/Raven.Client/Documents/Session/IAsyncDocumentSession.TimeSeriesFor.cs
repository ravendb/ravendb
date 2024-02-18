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
        /// <summary>
        /// Provides access to the TimeSeries API for performing server-side operations such as Append, Get, and Delete on a specific Time Series.
        /// </summary>
        IAsyncSessionDocumentTimeSeries TimeSeriesFor(string documentId, string name);

        /// <inheritdoc cref="TimeSeriesFor"/>
        IAsyncSessionDocumentTimeSeries TimeSeriesFor(object entity, string name);

        /// <inheritdoc cref="TimeSeriesFor"/>
        /// <typeparam name="TValues">The type of values stored in the Time Series.</typeparam>
        IAsyncSessionDocumentTypedTimeSeries<TValues> TimeSeriesFor<TValues>(string documentId, string name = null) where TValues : new();

        /// <inheritdoc cref="TimeSeriesFor{TValues}"/>
        IAsyncSessionDocumentTypedTimeSeries<TValues> TimeSeriesFor<TValues>(object entity, string name = null) where TValues : new();

        /// <inheritdoc cref="TimeSeriesFor{TValues}"/>
        IAsyncSessionDocumentRollupTypedTimeSeries<TValues> TimeSeriesRollupFor<TValues>(object entity, string policy, string raw = null) where TValues : new();

        /// <inheritdoc cref="TimeSeriesFor{TValues}"/>
        IAsyncSessionDocumentRollupTypedTimeSeries<TValues> TimeSeriesRollupFor<TValues>(string documentId, string policy, string raw = null) where TValues : new();

        /// <inheritdoc cref="TimeSeriesFor"/>
        IAsyncSessionDocumentIncrementalTimeSeries IncrementalTimeSeriesFor(string documentId, string name);

        /// <inheritdoc cref="TimeSeriesFor"/>
        IAsyncSessionDocumentIncrementalTimeSeries IncrementalTimeSeriesFor(object entity, string name);

        /// <inheritdoc cref="TimeSeriesFor{TValues}"/>
        IAsyncSessionDocumentTypedIncrementalTimeSeries<TValues> IncrementalTimeSeriesFor<TValues>(string documentId, string name = null) where TValues : new();

        /// <inheritdoc cref="TimeSeriesFor{TValues}"/>
        IAsyncSessionDocumentTypedIncrementalTimeSeries<TValues> IncrementalTimeSeriesFor<TValues>(object entity, string name = null) where TValues : new();
    }
}
