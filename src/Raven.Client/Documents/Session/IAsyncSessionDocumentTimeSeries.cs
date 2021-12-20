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
        Task<TimeSeriesEntry[]> GetAsync(DateTime? from = null, DateTime? to = null, int start = 0, int pageSize = int.MaxValue,
            CancellationToken token = default);

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
        Task<TimeSeriesRollupEntry<TValues>[]> GetAsync(DateTime? from = null, DateTime? to = null, int start = 0, int pageSize = int.MaxValue,
            CancellationToken token = default);
    }

    /// <summary>
    ///     Advanced async TimeSeries typed session operations
    /// </summary>
    public interface IAsyncSessionDocumentTypedIncrementalTimeSeries<TValues> :
        IAsyncTimeSeriesStreamingBase<TimeSeriesEntry<TValues>>,
        ISessionDocumentDeleteTimeSeriesBase,
        ISessionDocumentIncrementTimeSeriesBase
        where TValues : new()
    {
        Task<TimeSeriesEntry<TValues>[]> GetAsync(DateTime? from = null, DateTime? to = null, int start = 0, int pageSize = int.MaxValue,
            CancellationToken token = default);
    }

    /// <summary>
    ///     Advanced async TimeSeries session operations
    /// </summary>
    public interface IAsyncSessionDocumentIncrementalTimeSeries :
        ISessionDocumentIncrementTimeSeriesBase
    {
        Task<TimeSeriesEntry[]> GetAsync(DateTime? from = null, DateTime? to = null, int start = 0, int pageSize = int.MaxValue,
            CancellationToken token = default);

        Task<TimeSeriesEntry[]> GetAsync(DateTime? from, DateTime? to, Action<ITimeSeriesIncludeBuilder> includes, int start = 0, int pageSize = int.MaxValue,
            CancellationToken token = default);
    }
}
