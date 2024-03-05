//-----------------------------------------------------------------------
// <copyright file="IAsyncDocumentSession.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------


namespace Raven.Client.Documents.Session
{
    /// <summary>
    /// Provides an access to DocumentSession Time Series API.
    /// </summary>
    public partial interface IAsyncDocumentSession
    {
        /// <summary>
        /// Provides access to the Time Series API for performing server-side operations such as Append, Get, and Delete on a specific time series.
        /// </summary>
        /// <remarks>
        /// For more information on the Time Series Client API, see: <inheritdoc cref="DocumentationUrls.Session.TimeSeries.ClientApi"/>.<br/>
        /// </remarks>
        /// <param name="documentId">The unique identifier of the document associated with the time series.</param>
        /// <param name="name">The name of the time series to access.</param>
        IAsyncSessionDocumentTimeSeries TimeSeriesFor(string documentId, string name);

        /// <inheritdoc cref="TimeSeriesFor"/>
        /// <param name="entity">The document object associated with the time series.</param>
        /// <param name="name">The name of the time series to access.</param>
        IAsyncSessionDocumentTimeSeries TimeSeriesFor(object entity, string name);

        /// <summary>
        /// Provides access to the Time Series API for performing server-side operations such as Append, Get, and Delete on a specific time series.
        /// </summary>
        /// <remarks>
        /// For more information on the Time Series Client API, see: <inheritdoc cref="DocumentationUrls.Session.TimeSeries.ClientApi"/>.<br/>
        /// For details about named time series values, refer to: <inheritdoc cref="DocumentationUrls.Session.TimeSeries.NamedValues"/>.
        /// </remarks>
        /// <typeparam name="TValues">The type of values stored in the time series.</typeparam>
        /// <param name="documentId">The unique identifier of the document associated with the time series.</param>
        /// <param name="name">The name of the time series to access.</param>
        IAsyncSessionDocumentTypedTimeSeries<TValues> TimeSeriesFor<TValues>(string documentId, string name = null) where TValues : new();

        /// <summary>
        /// Provides access to the Time Series API for performing server-side operations such as Append, Get, and Delete on a specific time series.
        /// </summary>
        /// <remarks>
        /// For more information on the Time Series Client API, see: <inheritdoc cref="DocumentationUrls.Session.TimeSeries.ClientApi"/>.<br/>
        /// For details about named time series values, refer to: <inheritdoc cref="DocumentationUrls.Session.TimeSeries.NamedValues"/>.
        /// </remarks>
        /// <typeparam name="TValues">The type of values stored in the time series.</typeparam>
        /// <param name="entity">The document object associated with the time series.</param>
        /// <param name="name">The name of the time series to access.</param>
        IAsyncSessionDocumentTypedTimeSeries<TValues> TimeSeriesFor<TValues>(object entity, string name = null) where TValues : new();

        /// <summary>
        /// Provides access to the Time Series API for performing server-side operations such as Append, Get, and Delete on a specific rollup time series.
        /// </summary>
        /// <remarks>
        /// For more information on Time Series Rollup and Retention, see: <inheritdoc cref="DocumentationUrls.Session.TimeSeries.RollupAndRetention"/>.<br/>
        /// For details about named time series values, refer to: <inheritdoc cref="DocumentationUrls.Session.TimeSeries.NamedValues"/>.
        /// </remarks>
        /// <typeparam name="TValues">The type of values stored in the time series.</typeparam>
        /// <param name="entity">The document object associated with the time series.</param>
        /// <param name="policy">The policy name. The policy name indicates the time series rollup name.</param>
        /// <param name="raw">The name of the time series to access.</param>
        IAsyncSessionDocumentRollupTypedTimeSeries<TValues> TimeSeriesRollupFor<TValues>(object entity, string policy, string raw = null) where TValues : new();

        /// <inheritdoc cref="TimeSeriesRollupFor{TValues}"/>
        /// <param name="documentId">The unique identifier of the document associated with the time series.</param>
        IAsyncSessionDocumentRollupTypedTimeSeries<TValues> TimeSeriesRollupFor<TValues>(string documentId, string policy, string raw = null) where TValues : new();

        /// <summary>
        /// Provides access to the Incremental Time Series API for performing server-side operations such as Increment, Get, and Delete on a specific incremental time series.
        /// </summary>
        /// <remarks>
        /// For more information on Incremental Time Series, see: <inheritdoc cref="DocumentationUrls.Session.TimeSeries.IncrementalOverview"/>.
        /// </remarks>
        /// <param name="documentId">The unique identifier of the document associated with the incremental time series.</param>
        /// <param name="name">The name of the incremental time series to access. Name must start with INC: (can be either upper or lower case).</param>
        IAsyncSessionDocumentIncrementalTimeSeries IncrementalTimeSeriesFor(string documentId, string name);

        /// <inheritdoc cref="IncrementalTimeSeriesFor"/>
        /// <param name="entity">The document object associated with the incremental time series.</param>
        IAsyncSessionDocumentIncrementalTimeSeries IncrementalTimeSeriesFor(object entity, string name);

        /// <summary>
        /// Provides access to the Incremental Time Series API for performing server-side operations such as Increment, Get, and Delete on a specific incremental time series.
        /// </summary>
        /// <remarks>
        /// For more information on Incremental Time Series, see: <inheritdoc cref="DocumentationUrls.Session.TimeSeries.IncrementalOverview"/>.
        /// For details about named time series values, refer to: <inheritdoc cref="DocumentationUrls.Session.TimeSeries.NamedValues"/>.
        /// </remarks>
        /// <typeparam name="TValues">The type of values stored in the incremental time series.</typeparam>
        /// <param name="documentId">The unique identifier of the document associated with the incremental time series.</param>
        /// <param name="name">The name of the incremental time series to access. Name must start with INC: (can be either upper or lower case).</param>
        IAsyncSessionDocumentTypedIncrementalTimeSeries<TValues> IncrementalTimeSeriesFor<TValues>(string documentId, string name = null) where TValues : new();

        /// <inheritdoc cref="IncrementalTimeSeriesFor{TValue}"/>
        /// <param name="entity">The document object associated with the incremental time series.</param>
        IAsyncSessionDocumentTypedIncrementalTimeSeries<TValues> IncrementalTimeSeriesFor<TValues>(object entity, string name = null) where TValues : new();
    }
}
