//-----------------------------------------------------------------------
// <copyright file="IAsyncAdvancedSessionOperations.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Queries.TimeSeries;

namespace Raven.Client.Documents.Session
{
    /// <summary>
    ///     Advanced async session operations
    /// </summary>

    public partial interface IAsyncAdvancedSessionOperations :
        IAsyncAdvancedDocumentsSessionOperations,
        IAsyncTimeSeriesSessionStreamOperations<TimeSeriesAggregationResult>,
        IAsyncTimeSeriesSessionStreamOperations<TimeSeriesRawResult>,
        IAsyncTimeSeriesSessionStreamAggregationResultOperations,
        IAsyncTimeSeriesSessionStreamRawResultOperations
    {

    }

    public interface IAsyncTimeSeriesSessionStreamOperations<T>
    {
        /// <inheritdoc cref="IAsyncAdvancedDocumentsSessionOperations.StreamAsync{T}(IQueryable{T}, CancellationToken) "/>
        Task<IAsyncEnumerator<TimeSeriesStreamResult<T>>> StreamAsync(IQueryable<T> query, CancellationToken token = default);

        /// <inheritdoc cref="IAsyncAdvancedDocumentsSessionOperations.StreamAsync{T}(IQueryable{T}, out StreamQueryStatistics, CancellationToken) "/>
        Task<IAsyncEnumerator<TimeSeriesStreamResult<T>>> StreamAsync(IQueryable<T> query, out StreamQueryStatistics streamQueryStats, CancellationToken token = default);

        /// <inheritdoc cref="IAsyncAdvancedDocumentsSessionOperations.StreamAsync{T}(IAsyncDocumentQuery{T}, CancellationToken) "/>
        Task<IAsyncEnumerator<TimeSeriesStreamResult<T>>> StreamAsync(IAsyncDocumentQuery<T> query, CancellationToken token = default);

        /// <inheritdoc cref="IAsyncAdvancedDocumentsSessionOperations.StreamAsync{T}(IAsyncRawDocumentQuery{T}, CancellationToken) "/>
        Task<IAsyncEnumerator<TimeSeriesStreamResult<T>>> StreamAsync(IAsyncRawDocumentQuery<T> query, CancellationToken token = default);

        /// <inheritdoc cref="IAsyncAdvancedDocumentsSessionOperations.StreamAsync{T}(IAsyncRawDocumentQuery{T}, out StreamQueryStatistics, CancellationToken) "/>
        Task<IAsyncEnumerator<TimeSeriesStreamResult<T>>> StreamAsync(IAsyncRawDocumentQuery<T> query, out StreamQueryStatistics streamQueryStats, CancellationToken token = default);

        /// <inheritdoc cref="IAsyncAdvancedDocumentsSessionOperations.StreamAsync{T}(IAsyncDocumentQuery{T}, out StreamQueryStatistics, CancellationToken) "/>
        Task<IAsyncEnumerator<TimeSeriesStreamResult<T>>> StreamAsync(IAsyncDocumentQuery<T> query, out StreamQueryStatistics streamQueryStats, CancellationToken token = default);
    }

    public interface IAsyncTimeSeriesSessionStreamAggregationResultOperations
    {
        /// <inheritdoc cref="IAsyncAdvancedDocumentsSessionOperations.StreamAsync{T}(IQueryable{T}, CancellationToken) "/>
        Task<IAsyncEnumerator<TimeSeriesStreamResult<TimeSeriesAggregationResult<T>>>> StreamAsync<T>(IQueryable<TimeSeriesAggregationResult<T>> query, CancellationToken token = default) where T : new();

        /// <inheritdoc cref="IAsyncAdvancedDocumentsSessionOperations.StreamAsync{T}(IQueryable{T}, out StreamQueryStatistics, CancellationToken) "/>
        Task<IAsyncEnumerator<TimeSeriesStreamResult<TimeSeriesAggregationResult<T>>>> StreamAsync<T>(IQueryable<TimeSeriesAggregationResult<T>> query, out StreamQueryStatistics streamQueryStats, CancellationToken token = default) where T : new();

        /// <inheritdoc cref="IAsyncAdvancedDocumentsSessionOperations.StreamAsync{T}(IAsyncDocumentQuery{T}, CancellationToken) "/>
        Task<IAsyncEnumerator<TimeSeriesStreamResult<TimeSeriesAggregationResult<T>>>> StreamAsync<T>(IAsyncDocumentQuery<TimeSeriesAggregationResult<T>> query, CancellationToken token = default) where T : new();

        /// <inheritdoc cref="IAsyncAdvancedDocumentsSessionOperations.StreamAsync{T}(IAsyncRawDocumentQuery{T}, CancellationToken) "/>
        Task<IAsyncEnumerator<TimeSeriesStreamResult<TimeSeriesAggregationResult<T>>>> StreamAsync<T>(IAsyncRawDocumentQuery<TimeSeriesAggregationResult<T>> query, CancellationToken token = default) where T : new();

        /// <inheritdoc cref="IAsyncAdvancedDocumentsSessionOperations.StreamAsync{T}(IAsyncRawDocumentQuery{T}, out StreamQueryStatistics, CancellationToken) "/>
        Task<IAsyncEnumerator<TimeSeriesStreamResult<TimeSeriesAggregationResult<T>>>> StreamAsync<T>(IAsyncRawDocumentQuery<TimeSeriesAggregationResult<T>> query, out StreamQueryStatistics streamQueryStats, CancellationToken token = default) where T : new();

        /// <inheritdoc cref="IAsyncAdvancedDocumentsSessionOperations.StreamAsync{T}(IAsyncDocumentQuery{T}, out StreamQueryStatistics, CancellationToken) "/>
        Task<IAsyncEnumerator<TimeSeriesStreamResult<TimeSeriesAggregationResult<T>>>> StreamAsync<T>(IAsyncDocumentQuery<TimeSeriesAggregationResult<T>> query, out StreamQueryStatistics streamQueryStats, CancellationToken token = default) where T : new();
    }

    public interface IAsyncTimeSeriesSessionStreamRawResultOperations
    {
        /// <inheritdoc cref="IAsyncAdvancedDocumentsSessionOperations.StreamAsync{T}(IQueryable{T}, CancellationToken) "/>
        Task<IAsyncEnumerator<TimeSeriesStreamResult<TimeSeriesRawResult<T>>>> StreamAsync<T>(IQueryable<TimeSeriesRawResult<T>> query, CancellationToken token = default) where T : new();

        /// <inheritdoc cref="IAsyncAdvancedDocumentsSessionOperations.StreamAsync{T}(IQueryable{T}, out StreamQueryStatistics, CancellationToken) "/>
        Task<IAsyncEnumerator<TimeSeriesStreamResult<TimeSeriesRawResult<T>>>> StreamAsync<T>(IQueryable<TimeSeriesRawResult<T>> query, out StreamQueryStatistics streamQueryStats, CancellationToken token = default) where T : new();

        /// <inheritdoc cref="IAsyncAdvancedDocumentsSessionOperations.StreamAsync{T}(IAsyncDocumentQuery{T}, CancellationToken) "/>
        Task<IAsyncEnumerator<TimeSeriesStreamResult<TimeSeriesRawResult<T>>>> StreamAsync<T>(IAsyncDocumentQuery<TimeSeriesRawResult<T>> query, CancellationToken token = default) where T : new();

        /// <inheritdoc cref="IAsyncAdvancedDocumentsSessionOperations.StreamAsync{T}(IAsyncRawDocumentQuery{T}, CancellationToken) "/>
        Task<IAsyncEnumerator<TimeSeriesStreamResult<TimeSeriesRawResult<T>>>> StreamAsync<T>(IAsyncRawDocumentQuery<TimeSeriesRawResult<T>> query, CancellationToken token = default) where T : new();

        /// <inheritdoc cref="IAsyncAdvancedDocumentsSessionOperations.StreamAsync{T}(IAsyncRawDocumentQuery{T}, out StreamQueryStatistics, CancellationToken) "/>
        Task<IAsyncEnumerator<TimeSeriesStreamResult<TimeSeriesRawResult<T>>>> StreamAsync<T>(IAsyncRawDocumentQuery<TimeSeriesRawResult<T>> query, out StreamQueryStatistics streamQueryStats, CancellationToken token = default) where T : new();

        /// <inheritdoc cref="IAsyncAdvancedDocumentsSessionOperations.StreamAsync{T}(IAsyncDocumentQuery{T}, out StreamQueryStatistics, CancellationToken) "/>
        Task<IAsyncEnumerator<TimeSeriesStreamResult<TimeSeriesRawResult<T>>>> StreamAsync<T>(IAsyncDocumentQuery<TimeSeriesRawResult<T>> query, out StreamQueryStatistics streamQueryStats, CancellationToken token = default) where T : new();
    }

    public interface IAsyncAdvancedDocumentsSessionOperations
    {
        /// <inheritdoc cref="IDocumentsSessionOperations.Stream{T}(IDocumentQuery{T}) "/>
        /// <param name="token">A cancellation token that can be used to cancel the asynchronous operation if needed. By default: CancellationToken.None.</param>
        Task<IAsyncEnumerator<StreamResult<T>>> StreamAsync<T>(IAsyncDocumentQuery<T> query, CancellationToken token = default);

        /// <inheritdoc cref="IDocumentsSessionOperations.Stream{T}(IDocumentQuery{T}, out StreamQueryStatistics) "/>
        /// <param name="token">A cancellation token that can be used to cancel the asynchronous operation if needed. By default: CancellationToken.None.</param>
        Task<IAsyncEnumerator<StreamResult<T>>> StreamAsync<T>(IAsyncDocumentQuery<T> query, out StreamQueryStatistics streamQueryStats, CancellationToken token = default);

        /// <inheritdoc cref="IDocumentsSessionOperations.Stream{T}(IRawDocumentQuery{T}) "/>
        /// <param name="token">A cancellation token that can be used to cancel the asynchronous operation if needed. By default: CancellationToken.None.</param>
        Task<IAsyncEnumerator<StreamResult<T>>> StreamAsync<T>(IAsyncRawDocumentQuery<T> query, CancellationToken token = default);

        /// <inheritdoc cref="IDocumentsSessionOperations.Stream{T}(IRawDocumentQuery{T}, out StreamQueryStatistics) "/>
        /// <param name="token">A cancellation token that can be used to cancel the asynchronous operation if needed. By default: CancellationToken.None.</param>
        Task<IAsyncEnumerator<StreamResult<T>>> StreamAsync<T>(IAsyncRawDocumentQuery<T> query, out StreamQueryStatistics streamQueryStats, CancellationToken token = default);

        /// <inheritdoc cref="IDocumentsSessionOperations.Stream{T}(IQueryable{T}) "/>
        /// <param name="token">A cancellation token that can be used to cancel the asynchronous operation if needed. By default: CancellationToken.None.</param>
        Task<IAsyncEnumerator<StreamResult<T>>> StreamAsync<T>(IQueryable<T> query, CancellationToken token = default);

        /// <inheritdoc cref="IDocumentsSessionOperations.Stream{T}(IQueryable{T}, out StreamQueryStatistics) "/>
        /// <param name="token">A cancellation token that can be used to cancel the asynchronous operation if needed. By default: CancellationToken.None.</param>
        Task<IAsyncEnumerator<StreamResult<T>>> StreamAsync<T>(IQueryable<T> query, out StreamQueryStatistics streamQueryStats, CancellationToken token = default);

        /// <inheritdoc cref="IDocumentsSessionOperations.Stream{T}(string, string, int, int, string) "/>
        /// <param name="token">A cancellation token that can be used to cancel the asynchronous operation if needed. By default: CancellationToken.None.</param>
        Task<IAsyncEnumerator<StreamResult<T>>> StreamAsync<T>(string startsWith, string matches = null, int start = 0, int pageSize = int.MaxValue, string startAfter = null, CancellationToken token = default);

        /// <inheritdoc cref="IDocumentsSessionOperations.StreamInto{T}(IDocumentQuery{T}, Stream) "/>
        /// <param name="token">A cancellation token that can be used to cancel the asynchronous operation if needed. By default: CancellationToken.None.</param>
        /// <returns>A task responsible for feeding the results into the given stream</returns>
        Task StreamIntoAsync<T>(IAsyncDocumentQuery<T> query, Stream output, CancellationToken token = default);

        /// <inheritdoc cref="IDocumentsSessionOperations.StreamInto{T}(IRawDocumentQuery{T}, Stream) "/>
        /// <param name="token">A cancellation token that can be used to cancel the asynchronous operation if needed. By default: CancellationToken.None.</param>
        /// <returns>A task responsible for feeding the results into the given stream</returns>
        Task StreamIntoAsync<T>(IAsyncRawDocumentQuery<T> query, Stream output, CancellationToken token = default);
    }
}
