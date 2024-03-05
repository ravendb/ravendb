//-----------------------------------------------------------------------
// <copyright file="ISessionDocumentTimeSeries.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using Raven.Client.Documents.Session.Loaders;
using Raven.Client.Documents.Session.TimeSeries;

namespace Raven.Client.Documents.Session
{
    /// <summary>
    ///     Time series synchronous session operations
    /// </summary>
    public interface ISessionDocumentTimeSeries :
        ITimeSeriesStreamingBase<TimeSeriesEntry>,
        ISessionDocumentAppendTimeSeriesBase,
        ISessionDocumentDeleteTimeSeriesBase
    {
        /// <inheritdoc cref="IAsyncSessionDocumentTimeSeries.GetAsync"/>
        TimeSeriesEntry[] Get(DateTime? from = null, DateTime? to = null, int start = 0, int pageSize = int.MaxValue);

        /// <inheritdoc cref="Get"/>
        /// <param name="includes">Action to include Tags or Documents.</param>
        TimeSeriesEntry[] Get(DateTime? from, DateTime? to, Action<ITimeSeriesIncludeBuilder> includes, int start = 0, int pageSize = int.MaxValue);
    }

    /// <summary>
    ///     Incremental time series synchronous session operations
    /// </summary>
    public interface ISessionDocumentIncrementalTimeSeries :
        ITimeSeriesStreamingBase<TimeSeriesEntry>,
        ISessionDocumentDeleteTimeSeriesBase,
        ISessionDocumentIncrementTimeSeriesBase
    {
        /// <inheritdoc cref="IAsyncSessionDocumentTimeSeries.GetAsync"/>
        TimeSeriesEntry[] Get(DateTime? from = null, DateTime? to = null, int start = 0, int pageSize = int.MaxValue);

        /// <inheritdoc cref="Get"/>
        /// <param name="includes">Action to include Tags or Documents.</param>
        TimeSeriesEntry[] Get(DateTime? from, DateTime? to, Action<ITimeSeriesIncludeBuilder> includes, int start = 0, int pageSize = int.MaxValue);

    }

    /// <summary>
    ///     Time series typed synchronous session operations
    /// </summary>
    public interface ISessionDocumentTypedTimeSeries<TValues> :
        ISessionDocumentTypedAppendTimeSeriesBase<TValues>,
        ITimeSeriesStreamingBase<TimeSeriesEntry<TValues>>,
        ISessionDocumentDeleteTimeSeriesBase
        where TValues : new()
    {
        /// <inheritdoc cref="IAsyncSessionDocumentTypedTimeSeries{TValue}.GetAsync"/>
        TimeSeriesEntry<TValues>[] Get(DateTime? from = null, DateTime? to = null, int start = 0, int pageSize = int.MaxValue);
    }

    /// <summary>
    ///     Time series typed synchronous session operations
    /// </summary>
    public interface ISessionDocumentRollupTypedTimeSeries<TValues> :
        ITimeSeriesStreamingBase<TimeSeriesRollupEntry<TValues>>,
        ISessionDocumentRollupTypedAppendTimeSeriesBase<TValues>,
        ISessionDocumentDeleteTimeSeriesBase
        where TValues : new()
    {
        /// <inheritdoc cref="IAsyncSessionDocumentRollupTypedTimeSeries{TValue}.GetAsync"/>
        TimeSeriesRollupEntry<TValues>[] Get(DateTime? from = null, DateTime? to = null, int start = 0, int pageSize = int.MaxValue);
    }

    /// <summary>
    ///     Incremental time series typed synchronous session operations
    /// </summary>
    public interface ISessionDocumentTypedIncrementalTimeSeries<TValues> :
        ITimeSeriesStreamingBase<TimeSeriesEntry<TValues>>,
        ISessionDocumentDeleteTimeSeriesBase,
        ISessionDocumentTypedIncrementTimeSeriesBase<TValues>
        where TValues : new()
    {
        /// <inheritdoc cref="IAsyncSessionDocumentTypedIncrementalTimeSeries{TValue}.GetAsync"/>
        TimeSeriesEntry<TValues>[] Get(DateTime? from = null, DateTime? to = null, int start = 0, int pageSize = int.MaxValue);
    }
}
