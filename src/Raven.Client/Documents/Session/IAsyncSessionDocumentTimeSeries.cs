//-----------------------------------------------------------------------
// <copyright file="IAsyncSessionDocumentTimeSeries.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Session.Loaders;
using Raven.Client.Documents.Session.TimeSeries;

namespace Raven.Client.Documents.Session
{
    /// <summary>
    ///     Advanced async TimeSeries session operations
    /// </summary>
    public interface IAsyncSessionDocumentTimeSeries :
        ISessionDocumentAppendTimeSeriesBase,
        ISessionDocumentDeleteTimeSeriesBase,
        IAsyncTimeSeriesStreamingBase<TimeSeriesEntry>
    {
        /// <summary>
        /// Get Time-Series entries asynchronously.
        /// </summary>
        /// <param name="from">Date time to start collecting from (included). By default: DateTime.MinValue.</param>
        /// <param name="to">Date time to stop collecting from (included). By default: DateTime.MaxValue.</param>
        /// <param name="start">Number of time series entries that should be skipped. By default: 0.</param>
        /// <param name="pageSize">Maximum number of time series entries that will be retrieved. By default: int.MaxValue.</param>
        /// <param name="token"></param>
        Task<TimeSeriesEntry[]> GetAsync(DateTime? from = null, DateTime? to = null, int start = 0, int pageSize = int.MaxValue,
            CancellationToken token = default);

        /// <inheritdoc cref="GetAsync"/>
        /// <param name="includes">Action to include Tags or Documents.</param>
        Task<TimeSeriesEntry[]> GetAsync(DateTime? from, DateTime? to, Action<ITimeSeriesIncludeBuilder> includes, int start = 0, int pageSize = int.MaxValue,
            CancellationToken token = default);
    }

    /// <summary>
    ///     Advanced async TimeSeries typed session operations
    /// </summary>
    public interface IAsyncSessionDocumentTypedTimeSeries<TValues> :
        ISessionDocumentTypedAppendTimeSeriesBase<TValues>,
        IAsyncTimeSeriesStreamingBase<TimeSeriesEntry<TValues>>,
        ISessionDocumentDeleteTimeSeriesBase
        where TValues : new()
    {
        /// <inheritdoc cref="IAsyncSessionDocumentTimeSeries.GetAsync"/>
        /// <returns>Time Series entries where the values are converted to the type indicated by <typeparamref name="TValues"/></returns>
        Task<TimeSeriesEntry<TValues>[]> GetAsync(DateTime? from = null, DateTime? to = null, int start = 0, int pageSize = int.MaxValue,
            CancellationToken token = default);
    }

    /// <summary>
    ///     Advanced async TimeSeries typed session operations
    /// </summary>
    public interface IAsyncSessionDocumentRollupTypedTimeSeries<TValues> :
        ISessionDocumentTypedAppendTimeSeriesBase<TValues>,
        IAsyncTimeSeriesStreamingBase<TimeSeriesRollupEntry<TValues>>,
        ISessionDocumentDeleteTimeSeriesBase
        where TValues : new()
    {
        /// <inheritdoc cref="IAsyncSessionDocumentTypedTimeSeries{TValues}.GetAsync"/>
        Task<TimeSeriesRollupEntry<TValues>[]> GetAsync(DateTime? from = null, DateTime? to = null, int start = 0, int pageSize = int.MaxValue,
            CancellationToken token = default);
    }

    /// <summary>
    ///     Advanced async Incremental TimeSeries typed session operations
    /// </summary>
    public interface IAsyncSessionDocumentTypedIncrementalTimeSeries<TValues> :
        IAsyncTimeSeriesStreamingBase<TimeSeriesEntry<TValues>>,
        ISessionDocumentDeleteTimeSeriesBase,
        ISessionDocumentIncrementTimeSeriesBase
        where TValues : new()
    {
        /// <inheritdoc cref="IAsyncSessionDocumentTypedTimeSeries{TValues}.GetAsync"/>
        Task<TimeSeriesEntry<TValues>[]> GetAsync(DateTime? from = null, DateTime? to = null, int start = 0, int pageSize = int.MaxValue,
            CancellationToken token = default);
    }

    /// <summary>
    ///     Advanced async Incremental TimeSeries session operations
    /// </summary>
    public interface IAsyncSessionDocumentIncrementalTimeSeries :
        ISessionDocumentIncrementTimeSeriesBase
    {
        /// <inheritdoc cref="IAsyncSessionDocumentTimeSeries.GetAsync"/>
        Task<TimeSeriesEntry[]> GetAsync(DateTime? from = null, DateTime? to = null, int start = 0, int pageSize = int.MaxValue,
            CancellationToken token = default);

        /// <inheritdoc cref="IAsyncSessionDocumentTimeSeries.GetAsync(DateTime?, DateTime?, Action{ITimeSeriesIncludeBuilder}, int, int, CancellationToken)"/>
        Task<TimeSeriesEntry[]> GetAsync(DateTime? from, DateTime? to, Action<ITimeSeriesIncludeBuilder> includes, int start = 0, int pageSize = int.MaxValue,
            CancellationToken token = default);
    }
}
