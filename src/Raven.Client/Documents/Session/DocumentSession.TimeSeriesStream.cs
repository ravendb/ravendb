using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Queries.TimeSeries;
using Raven.Client.Documents.Session.Operations;

namespace Raven.Client.Documents.Session
{
    public partial class DocumentSession
    {
        private IEnumerator<TimeSeriesStreamResult<T>> StreamTimeSeriesInternal<T>(IDocumentQuery<T> query) where T : ITimeSeriesQueryStreamResult, new()
        {
            var streamOperation = new StreamOperation(this);
            var command = streamOperation.CreateRequest(query.GetIndexQuery());

            RequestExecutor.Execute(command, Context, sessionInfo: _sessionInfo);
            streamOperation.EnsureIsAcceptable(query.IndexName, command.Result);

            var result = streamOperation.SetResultForTimeSeries(command.Result);

            return YieldTimeSeriesResults(query, result);
        }
        
        private IEnumerator<TimeSeriesStreamResult<T>> StreamTimeSeriesInternal<T>(IDocumentQuery<T> query, out StreamQueryStatistics streamQueryStats) where T : ITimeSeriesQueryStreamResult, new()
        {
            var stats = new StreamQueryStatistics();
            var streamOperation = new StreamOperation(this, stats);
            var command = streamOperation.CreateRequest(query.GetIndexQuery());

            RequestExecutor.Execute(command, Context, sessionInfo: _sessionInfo);
            streamOperation.EnsureIsAcceptable(query.IndexName, command.Result);

            var result = streamOperation.SetResultForTimeSeries(command.Result);
            streamQueryStats = stats;

            return YieldTimeSeriesResults(query, result);
        }

        private IEnumerator<TimeSeriesStreamResult<T>> YieldTimeSeriesResults<T>(IDocumentQuery<T> query, StreamOperation.YieldStreamResults enumerator) where T : ITimeSeriesQueryStreamResult, new()
        {
            using (enumerator)
            {
                while (enumerator.MoveNext())
                {
                    using (var json = enumerator.Current)
                    {
                        query.InvokeAfterStreamExecuted(json);

                        yield return CreateTimeSeriesStreamResult<T>(enumerator);
                    }
                }
            }
        }

        private IEnumerator<TimeSeriesStreamResult<T>> StreamTimeSeriesInternal<T>(IRawDocumentQuery<T> query) where T : ITimeSeriesQueryStreamResult, new()
        {
            return StreamTimeSeriesInternal((IDocumentQuery<T>)query);
        }

        private IEnumerator<TimeSeriesStreamResult<T>> StreamTimeSeriesInternal<T>(IQueryable<T> query) where T : ITimeSeriesQueryStreamResult, new()
        {
            var queryProvider = (IRavenQueryProvider)query.Provider;
            var docQuery = queryProvider.ToDocumentQuery<T>(query.Expression);
            return StreamTimeSeriesInternal(docQuery);
        }

        private IEnumerator<TimeSeriesStreamResult<T>> StreamTimeSeriesInternal<T>(IQueryable<T> query, out StreamQueryStatistics streamQueryStats) where T : ITimeSeriesQueryStreamResult, new()
        {
            var queryProvider = (IRavenQueryProvider)query.Provider;
            var docQuery = queryProvider.ToDocumentQuery<T>(query.Expression);
            return StreamTimeSeriesInternal(docQuery, out streamQueryStats);
        }

        private IEnumerator<TimeSeriesStreamResult<T>> StreamTimeSeriesInternal<T>(IRawDocumentQuery<T> query, out StreamQueryStatistics streamQueryStats) where T : ITimeSeriesQueryStreamResult, new()
        {
            return StreamTimeSeriesInternal((IDocumentQuery<T>)query, out streamQueryStats);
        }

        IEnumerator<TimeSeriesStreamResult<TimeSeriesAggregationResult>> ITimeSeriesSessionStreamOperations<TimeSeriesAggregationResult>.Stream(IQueryable<TimeSeriesAggregationResult> query)
        {
            return StreamTimeSeriesInternal(query);
        }

        IEnumerator<TimeSeriesStreamResult<TimeSeriesAggregationResult>> ITimeSeriesSessionStreamOperations<TimeSeriesAggregationResult>.Stream(IQueryable<TimeSeriesAggregationResult> query, out StreamQueryStatistics streamQueryStats)
        {
            return StreamTimeSeriesInternal(query, out streamQueryStats);
        }

        IEnumerator<TimeSeriesStreamResult<TimeSeriesAggregationResult>> ITimeSeriesSessionStreamOperations<TimeSeriesAggregationResult>.Stream(IDocumentQuery<TimeSeriesAggregationResult> query)
        {
            return StreamTimeSeriesInternal(query);
        }

        IEnumerator<TimeSeriesStreamResult<TimeSeriesAggregationResult>> ITimeSeriesSessionStreamOperations<TimeSeriesAggregationResult>.Stream(IRawDocumentQuery<TimeSeriesAggregationResult> query)
        {
            return StreamTimeSeriesInternal(query);
        }

        IEnumerator<TimeSeriesStreamResult<TimeSeriesAggregationResult>> ITimeSeriesSessionStreamOperations<TimeSeriesAggregationResult>.Stream(IRawDocumentQuery<TimeSeriesAggregationResult> query, out StreamQueryStatistics streamQueryStats)
        {
            return StreamTimeSeriesInternal(query, out streamQueryStats);
        }

        IEnumerator<TimeSeriesStreamResult<TimeSeriesAggregationResult>> ITimeSeriesSessionStreamOperations<TimeSeriesAggregationResult>.Stream(IDocumentQuery<TimeSeriesAggregationResult> query, out StreamQueryStatistics streamQueryStats)
        {
            return StreamTimeSeriesInternal(query, out streamQueryStats);
        }

        IEnumerator<TimeSeriesStreamResult<TimeSeriesRawResult>> ITimeSeriesSessionStreamOperations<TimeSeriesRawResult>.Stream(IQueryable<TimeSeriesRawResult> query)
        {
            return StreamTimeSeriesInternal(query);
        }

        IEnumerator<TimeSeriesStreamResult<TimeSeriesRawResult>> ITimeSeriesSessionStreamOperations<TimeSeriesRawResult>.Stream(IQueryable<TimeSeriesRawResult> query, out StreamQueryStatistics streamQueryStats)
        {
            return StreamTimeSeriesInternal(query, out streamQueryStats);
        }

        IEnumerator<TimeSeriesStreamResult<TimeSeriesRawResult>> ITimeSeriesSessionStreamOperations<TimeSeriesRawResult>.Stream(IDocumentQuery<TimeSeriesRawResult> query)
        {
            return StreamTimeSeriesInternal(query);
        }

        IEnumerator<TimeSeriesStreamResult<TimeSeriesRawResult>> ITimeSeriesSessionStreamOperations<TimeSeriesRawResult>.Stream(IRawDocumentQuery<TimeSeriesRawResult> query)
        {
            return StreamTimeSeriesInternal(query);
        }

        IEnumerator<TimeSeriesStreamResult<TimeSeriesRawResult>> ITimeSeriesSessionStreamOperations<TimeSeriesRawResult>.Stream(IRawDocumentQuery<TimeSeriesRawResult> query, out StreamQueryStatistics streamQueryStats)
        {
            return StreamTimeSeriesInternal(query, out streamQueryStats);
        }

        IEnumerator<TimeSeriesStreamResult<TimeSeriesRawResult>> ITimeSeriesSessionStreamOperations<TimeSeriesRawResult>.Stream(IDocumentQuery<TimeSeriesRawResult> query, out StreamQueryStatistics streamQueryStats)
        {
            return StreamTimeSeriesInternal(query, out streamQueryStats);
        }

        IEnumerator<TimeSeriesStreamResult<TimeSeriesAggregationResult<T>>> ITimeSeriesSessionStreamAggregationResultOperations.Stream<T>(IQueryable<TimeSeriesAggregationResult<T>> query) 
        {
            return StreamTimeSeriesInternal(query);
        }

        IEnumerator<TimeSeriesStreamResult<TimeSeriesAggregationResult<T>>> ITimeSeriesSessionStreamAggregationResultOperations.Stream<T>(IQueryable<TimeSeriesAggregationResult<T>> query, out StreamQueryStatistics streamQueryStats) 
        {
            return StreamTimeSeriesInternal(query, out streamQueryStats);
        }

        IEnumerator<TimeSeriesStreamResult<TimeSeriesAggregationResult<T>>> ITimeSeriesSessionStreamAggregationResultOperations.Stream<T>(IDocumentQuery<TimeSeriesAggregationResult<T>> query) 
        {
            return StreamTimeSeriesInternal(query);
        }

        IEnumerator<TimeSeriesStreamResult<TimeSeriesAggregationResult<T>>> ITimeSeriesSessionStreamAggregationResultOperations.Stream<T>(IRawDocumentQuery<TimeSeriesAggregationResult<T>> query) 
        {
            return StreamTimeSeriesInternal(query);
        }

        IEnumerator<TimeSeriesStreamResult<TimeSeriesAggregationResult<T>>> ITimeSeriesSessionStreamAggregationResultOperations.Stream<T>(IRawDocumentQuery<TimeSeriesAggregationResult<T>> query, out StreamQueryStatistics streamQueryStats) 
        {
            return StreamTimeSeriesInternal(query, out streamQueryStats);
        }

        IEnumerator<TimeSeriesStreamResult<TimeSeriesAggregationResult<T>>> ITimeSeriesSessionStreamAggregationResultOperations.Stream<T>(IDocumentQuery<TimeSeriesAggregationResult<T>> query, out StreamQueryStatistics streamQueryStats) 
        {
            return StreamTimeSeriesInternal(query, out streamQueryStats);
        }

        IEnumerator<TimeSeriesStreamResult<TimeSeriesRawResult<T>>> ITimeSeriesSessionStreamRawResultOperations.Stream<T>(IQueryable<TimeSeriesRawResult<T>> query) 
        {
            return StreamTimeSeriesInternal(query);
        }

        IEnumerator<TimeSeriesStreamResult<TimeSeriesRawResult<T>>> ITimeSeriesSessionStreamRawResultOperations.Stream<T>(IQueryable<TimeSeriesRawResult<T>> query, out StreamQueryStatistics streamQueryStats) 
        {
            return StreamTimeSeriesInternal(query, out streamQueryStats);
        }

        IEnumerator<TimeSeriesStreamResult<TimeSeriesRawResult<T>>> ITimeSeriesSessionStreamRawResultOperations.Stream<T>(IDocumentQuery<TimeSeriesRawResult<T>> query) 
        {
            return StreamTimeSeriesInternal(query);
        }

        IEnumerator<TimeSeriesStreamResult<TimeSeriesRawResult<T>>> ITimeSeriesSessionStreamRawResultOperations.Stream<T>(IRawDocumentQuery<TimeSeriesRawResult<T>> query) 
        {
            return StreamTimeSeriesInternal(query);
        }

        IEnumerator<TimeSeriesStreamResult<TimeSeriesRawResult<T>>> ITimeSeriesSessionStreamRawResultOperations.Stream<T>(IRawDocumentQuery<TimeSeriesRawResult<T>> query, out StreamQueryStatistics streamQueryStats) 
        {
            return StreamTimeSeriesInternal(query, out streamQueryStats);
        }

        IEnumerator<TimeSeriesStreamResult<TimeSeriesRawResult<T>>> ITimeSeriesSessionStreamRawResultOperations.Stream<T>(IDocumentQuery<TimeSeriesRawResult<T>> query, out StreamQueryStatistics streamQueryStats) 
        {
            return StreamTimeSeriesInternal(query, out streamQueryStats);
        }
    }
}
