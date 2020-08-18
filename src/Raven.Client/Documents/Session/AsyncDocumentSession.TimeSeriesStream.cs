using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Queries.TimeSeries;
using Raven.Client.Documents.Session.Operations;

namespace Raven.Client.Documents.Session
{
    public partial class AsyncDocumentSession
    {
        private async Task<IAsyncEnumerator<TimeSeriesStreamResult<T>>> TimeSeriesStreamInternalAsync<T>(IAsyncDocumentQuery<T> query, StreamQueryStatistics streamQueryStats, CancellationToken token = default)
        {
            using (AsyncTaskHolder())
            {
                var documentQuery = (AsyncDocumentQuery<T>)query;
                var indexQuery = query.GetIndexQuery();

                var streamOperation = new StreamOperation(this, streamQueryStats);
                var command = streamOperation.CreateRequest(indexQuery);
                await RequestExecutor.ExecuteAsync(command, Context, _sessionInfo, token).ConfigureAwait(false);
                var result = await streamOperation.SetResultForTimeSeriesAsync(command.Result).ConfigureAwait(false);

                var queryOperation = ((AsyncDocumentQuery<T>)query).InitializeQueryOperation();
                queryOperation.NoTracking = true;
                return new YieldTimeSeriesStream<T>(this, documentQuery, result, token);
            }
        }

        public class YieldTimeSeriesStream<T> : AbstractYieldStream<TimeSeriesStreamResult<T>>
        {
            internal YieldTimeSeriesStream(AsyncDocumentSession parent, AsyncDocumentQuery<T> query, StreamOperation.YieldStreamResults enumerator, CancellationToken token) : 
                base(enumerator, () =>
                {
                    query?.InvokeAfterStreamExecuted(enumerator.Current);
                    return parent.CreateTimeSeriesStreamResult<T>(enumerator);
                }, token)
            {
            }
        }

        private Task<IAsyncEnumerator<TimeSeriesStreamResult<T>>> TimeSeriesStreamInternalAsync<T>(IAsyncDocumentQuery<T> query, CancellationToken token)
        {
            return TimeSeriesStreamInternalAsync(query, null, token);
        }

        private Task<IAsyncEnumerator<TimeSeriesStreamResult<T>>> TimeSeriesStreamInternalAsync<T>(IAsyncDocumentQuery<T> query, out StreamQueryStatistics streamQueryStats, CancellationToken token = default)
        {
            streamQueryStats = new StreamQueryStatistics();
            return TimeSeriesStreamInternalAsync(query, streamQueryStats, token);
        }

        private Task<IAsyncEnumerator<TimeSeriesStreamResult<T>>> TimeSeriesStreamInternalAsync<T>(IQueryable<T> query, CancellationToken token)
        {
            var queryInspector = (IRavenQueryProvider)query.Provider;
            var indexQuery = queryInspector.ToAsyncDocumentQuery<T>(query.Expression);
            return TimeSeriesStreamInternalAsync(indexQuery, token);
        }

        private Task<IAsyncEnumerator<TimeSeriesStreamResult<T>>> TimeSeriesStreamInternalAsync<T>(IAsyncRawDocumentQuery<T> query, CancellationToken token)
        {
            return TimeSeriesStreamInternalAsync((IAsyncDocumentQuery<T>)query, token);
        }

        private Task<IAsyncEnumerator<TimeSeriesStreamResult<T>>> TimeSeriesStreamInternalAsync<T>(IQueryable<T> query, out StreamQueryStatistics streamQueryStats, CancellationToken token = default)
        {
            var queryInspector = (IRavenQueryProvider)query.Provider;
            var indexQuery = queryInspector.ToAsyncDocumentQuery<T>(query.Expression);
            return TimeSeriesStreamInternalAsync(indexQuery, out streamQueryStats, token);
        }
       
        private Task<IAsyncEnumerator<TimeSeriesStreamResult<T>>> TimeSeriesStreamInternalAsync<T>(IAsyncRawDocumentQuery<T> query, out StreamQueryStatistics streamQueryStats, CancellationToken token = default)
        {
            return TimeSeriesStreamInternalAsync((IAsyncDocumentQuery<T>)query, out streamQueryStats, token);
        }

        public Task<IAsyncEnumerator<TimeSeriesStreamResult<TimeSeriesAggregationResult>>> StreamAsync(IQueryable<TimeSeriesAggregationResult> query, CancellationToken token = default)
        {
            return TimeSeriesStreamInternalAsync(query, token);
        }

        public Task<IAsyncEnumerator<TimeSeriesStreamResult<TimeSeriesAggregationResult>>> StreamAsync(IQueryable<TimeSeriesAggregationResult> query, out StreamQueryStatistics streamQueryStats, CancellationToken token = default)
        {
            return TimeSeriesStreamInternalAsync(query, out streamQueryStats, token);
        }

        public Task<IAsyncEnumerator<TimeSeriesStreamResult<TimeSeriesAggregationResult>>> StreamAsync(IAsyncDocumentQuery<TimeSeriesAggregationResult> query, CancellationToken token = default)
        {
            return TimeSeriesStreamInternalAsync(query, token);
        }

        public Task<IAsyncEnumerator<TimeSeriesStreamResult<TimeSeriesAggregationResult>>> StreamAsync(IAsyncRawDocumentQuery<TimeSeriesAggregationResult> query, CancellationToken token = default)
        {
            return TimeSeriesStreamInternalAsync(query, token);
        }

        public Task<IAsyncEnumerator<TimeSeriesStreamResult<TimeSeriesAggregationResult>>> StreamAsync(IAsyncRawDocumentQuery<TimeSeriesAggregationResult> query, out StreamQueryStatistics streamQueryStats, CancellationToken token = default)
        {
            return TimeSeriesStreamInternalAsync(query, out streamQueryStats, token);
        }

        public Task<IAsyncEnumerator<TimeSeriesStreamResult<TimeSeriesAggregationResult>>> StreamAsync(IAsyncDocumentQuery<TimeSeriesAggregationResult> query, out StreamQueryStatistics streamQueryStats, CancellationToken token = default)
        {
            return TimeSeriesStreamInternalAsync(query, out streamQueryStats, token);
        }

        public Task<IAsyncEnumerator<TimeSeriesStreamResult<TimeSeriesRawResult>>> StreamAsync(IQueryable<TimeSeriesRawResult> query, CancellationToken token = default)
        {
            return TimeSeriesStreamInternalAsync(query, token);
        }

        public Task<IAsyncEnumerator<TimeSeriesStreamResult<TimeSeriesRawResult>>> StreamAsync(IQueryable<TimeSeriesRawResult> query, out StreamQueryStatistics streamQueryStats, CancellationToken token = default)
        {
            return TimeSeriesStreamInternalAsync(query, out streamQueryStats, token);
        }

        public Task<IAsyncEnumerator<TimeSeriesStreamResult<TimeSeriesRawResult>>> StreamAsync(IAsyncDocumentQuery<TimeSeriesRawResult> query, CancellationToken token = default)
        {
            return TimeSeriesStreamInternalAsync(query, token);
        }

        public Task<IAsyncEnumerator<TimeSeriesStreamResult<TimeSeriesRawResult>>> StreamAsync(IAsyncRawDocumentQuery<TimeSeriesRawResult> query, CancellationToken token = default)
        {
            return TimeSeriesStreamInternalAsync(query, token);
        }

        public Task<IAsyncEnumerator<TimeSeriesStreamResult<TimeSeriesRawResult>>> StreamAsync(IAsyncRawDocumentQuery<TimeSeriesRawResult> query, out StreamQueryStatistics streamQueryStats, CancellationToken token = default)
        {
            return TimeSeriesStreamInternalAsync(query, out streamQueryStats, token);
        }

        public Task<IAsyncEnumerator<TimeSeriesStreamResult<TimeSeriesRawResult>>> StreamAsync(IAsyncDocumentQuery<TimeSeriesRawResult> query, out StreamQueryStatistics streamQueryStats, CancellationToken token = default)
        {
            return TimeSeriesStreamInternalAsync(query, out streamQueryStats, token);
        }

        public Task<IAsyncEnumerator<TimeSeriesStreamResult<TimeSeriesAggregationResult<T>>>> StreamAsync<T>(IQueryable<TimeSeriesAggregationResult<T>> query, CancellationToken token = default) where T : new()
        {
            return TimeSeriesStreamInternalAsync(query, token);
        }

        public Task<IAsyncEnumerator<TimeSeriesStreamResult<TimeSeriesAggregationResult<T>>>> StreamAsync<T>(IQueryable<TimeSeriesAggregationResult<T>> query, out StreamQueryStatistics streamQueryStats, CancellationToken token = default) where T : new()
        {
            return TimeSeriesStreamInternalAsync(query, out streamQueryStats, token);
        }

        public Task<IAsyncEnumerator<TimeSeriesStreamResult<TimeSeriesAggregationResult<T>>>> StreamAsync<T>(IAsyncDocumentQuery<TimeSeriesAggregationResult<T>> query, CancellationToken token = default) where T : new()
        {
            return TimeSeriesStreamInternalAsync(query, token);
        }

        public Task<IAsyncEnumerator<TimeSeriesStreamResult<TimeSeriesAggregationResult<T>>>> StreamAsync<T>(IAsyncRawDocumentQuery<TimeSeriesAggregationResult<T>> query, CancellationToken token = default) where T : new()
        {
            return TimeSeriesStreamInternalAsync(query, token);
        }

        public Task<IAsyncEnumerator<TimeSeriesStreamResult<TimeSeriesAggregationResult<T>>>> StreamAsync<T>(IAsyncRawDocumentQuery<TimeSeriesAggregationResult<T>> query, out StreamQueryStatistics streamQueryStats, CancellationToken token = default) where T : new()
        {
            return TimeSeriesStreamInternalAsync(query, out streamQueryStats, token);
        }

        public Task<IAsyncEnumerator<TimeSeriesStreamResult<TimeSeriesAggregationResult<T>>>> StreamAsync<T>(IAsyncDocumentQuery<TimeSeriesAggregationResult<T>> query, out StreamQueryStatistics streamQueryStats, CancellationToken token = default) where T : new()
        {
            return TimeSeriesStreamInternalAsync(query, out streamQueryStats, token);
        }

        public Task<IAsyncEnumerator<TimeSeriesStreamResult<TimeSeriesRawResult<T>>>> StreamAsync<T>(IQueryable<TimeSeriesRawResult<T>> query, CancellationToken token = default) where T : new()
        {
            return TimeSeriesStreamInternalAsync(query, token);
        }

        public Task<IAsyncEnumerator<TimeSeriesStreamResult<TimeSeriesRawResult<T>>>> StreamAsync<T>(IQueryable<TimeSeriesRawResult<T>> query, out StreamQueryStatistics streamQueryStats, CancellationToken token = default) where T : new()
        {
            return TimeSeriesStreamInternalAsync(query, out streamQueryStats, token);
        }

        public Task<IAsyncEnumerator<TimeSeriesStreamResult<TimeSeriesRawResult<T>>>> StreamAsync<T>(IAsyncDocumentQuery<TimeSeriesRawResult<T>> query, CancellationToken token = default) where T : new()
        {
            return TimeSeriesStreamInternalAsync(query, token);
        }

        public Task<IAsyncEnumerator<TimeSeriesStreamResult<TimeSeriesRawResult<T>>>> StreamAsync<T>(IAsyncRawDocumentQuery<TimeSeriesRawResult<T>> query, CancellationToken token = default) where T : new()
        {
            return TimeSeriesStreamInternalAsync(query, token);
        }

        public Task<IAsyncEnumerator<TimeSeriesStreamResult<TimeSeriesRawResult<T>>>> StreamAsync<T>(IAsyncRawDocumentQuery<TimeSeriesRawResult<T>> query, out StreamQueryStatistics streamQueryStats, CancellationToken token = default) where T : new()
        {
            return TimeSeriesStreamInternalAsync(query, out streamQueryStats, token);
        }

        public Task<IAsyncEnumerator<TimeSeriesStreamResult<TimeSeriesRawResult<T>>>> StreamAsync<T>(IAsyncDocumentQuery<TimeSeriesRawResult<T>> query, out StreamQueryStatistics streamQueryStats, CancellationToken token = default) where T : new()
        {
            return TimeSeriesStreamInternalAsync(query, out streamQueryStats, token);
        }
    }
}
