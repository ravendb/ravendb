using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Queries.TimeSeries;
using Raven.Client.Documents.Session.Operations;
using Raven.Client.Extensions;
using Raven.Client.Json;
using Sparrow.Json;

namespace Raven.Client.Documents.Session
{
    public partial class DocumentSession
    {
        private IEnumerator<TimeSeriesStreamResult<T>> StreamTimeSeriesInternal<T>(IDocumentQuery<T> query)
        {
            var streamOperation = new StreamOperation(this);
            var command = streamOperation.CreateRequest(query.GetIndexQuery());

            RequestExecutor.Execute(command, Context, sessionInfo: _sessionInfo);
            var result = streamOperation.SetResultForTimeSeries(command.Result);

            return YieldTimeSeriesResults(query, result);
        }
        
        private IEnumerator<TimeSeriesStreamResult<T>> StreamTimeSeriesInternal<T>(IDocumentQuery<T> query, out StreamQueryStatistics streamQueryStats)
        {
            var stats = new StreamQueryStatistics();
            var streamOperation = new StreamOperation(this, stats);
            var command = streamOperation.CreateRequest(query.GetIndexQuery());

            RequestExecutor.Execute(command, Context, sessionInfo: _sessionInfo);
            var result = streamOperation.SetResultForTimeSeries(command.Result);
            streamQueryStats = stats;

            return YieldTimeSeriesResults(query, result);
        }

        private IEnumerator<TimeSeriesStreamResult<T>> YieldTimeSeriesResults<T>(IDocumentQuery<T> query, StreamOperation.YieldStreamResults enumerator)
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

        private IEnumerator<TimeSeriesStreamResult<T>> StreamTimeSeriesInternal<T>(IRawDocumentQuery<T> query)
        {
            return StreamTimeSeriesInternal((IDocumentQuery<T>)query);
        }

        private IEnumerator<TimeSeriesStreamResult<T>> StreamTimeSeriesInternal<T>(IQueryable<T> query)
        {
            var queryProvider = (IRavenQueryProvider)query.Provider;
            var docQuery = queryProvider.ToDocumentQuery<T>(query.Expression);
            return StreamTimeSeriesInternal(docQuery);
        }

        private IEnumerator<TimeSeriesStreamResult<T>> StreamTimeSeriesInternal<T>(IQueryable<T> query, out StreamQueryStatistics streamQueryStats)
        {
            var queryProvider = (IRavenQueryProvider)query.Provider;
            var docQuery = queryProvider.ToDocumentQuery<T>(query.Expression);
            return StreamTimeSeriesInternal(docQuery, out streamQueryStats);
        }

        private IEnumerator<TimeSeriesStreamResult<T>> StreamTimeSeriesInternal<T>(IRawDocumentQuery<T> query, out StreamQueryStatistics streamQueryStats)
        {
            return StreamTimeSeriesInternal((IDocumentQuery<T>)query, out streamQueryStats);
        }

        public IEnumerator<TimeSeriesStreamResult<TimeSeriesAggregationResult>> Stream(IQueryable<TimeSeriesAggregationResult> query)
        {
            return StreamTimeSeriesInternal(query);
        }

        public IEnumerator<TimeSeriesStreamResult<TimeSeriesAggregationResult>> Stream(IQueryable<TimeSeriesAggregationResult> query, out StreamQueryStatistics streamQueryStats)
        {
            return StreamTimeSeriesInternal(query, out streamQueryStats);
        }

        public IEnumerator<TimeSeriesStreamResult<TimeSeriesAggregationResult>> Stream(IDocumentQuery<TimeSeriesAggregationResult> query)
        {
            return StreamTimeSeriesInternal(query);
        }

        public IEnumerator<TimeSeriesStreamResult<TimeSeriesAggregationResult>> Stream(IRawDocumentQuery<TimeSeriesAggregationResult> query)
        {
            return StreamTimeSeriesInternal(query);
        }

        public IEnumerator<TimeSeriesStreamResult<TimeSeriesAggregationResult>> Stream(IRawDocumentQuery<TimeSeriesAggregationResult> query, out StreamQueryStatistics streamQueryStats)
        {
            return StreamTimeSeriesInternal(query, out streamQueryStats);
        }

        public IEnumerator<TimeSeriesStreamResult<TimeSeriesAggregationResult>> Stream(IDocumentQuery<TimeSeriesAggregationResult> query, out StreamQueryStatistics streamQueryStats)
        {
            return StreamTimeSeriesInternal(query, out streamQueryStats);
        }

        public IEnumerator<TimeSeriesStreamResult<TimeSeriesRawResult>> Stream(IQueryable<TimeSeriesRawResult> query)
        {
            return StreamTimeSeriesInternal(query);
        }

        public IEnumerator<TimeSeriesStreamResult<TimeSeriesRawResult>> Stream(IQueryable<TimeSeriesRawResult> query, out StreamQueryStatistics streamQueryStats)
        {
            return StreamTimeSeriesInternal(query, out streamQueryStats);
        }

        public IEnumerator<TimeSeriesStreamResult<TimeSeriesRawResult>> Stream(IDocumentQuery<TimeSeriesRawResult> query)
        {
            return StreamTimeSeriesInternal(query);
        }

        public IEnumerator<TimeSeriesStreamResult<TimeSeriesRawResult>> Stream(IRawDocumentQuery<TimeSeriesRawResult> query)
        {
            return StreamTimeSeriesInternal(query);
        }

        public IEnumerator<TimeSeriesStreamResult<TimeSeriesRawResult>> Stream(IRawDocumentQuery<TimeSeriesRawResult> query, out StreamQueryStatistics streamQueryStats)
        {
            return StreamTimeSeriesInternal(query, out streamQueryStats);
        }

        public IEnumerator<TimeSeriesStreamResult<TimeSeriesRawResult>> Stream(IDocumentQuery<TimeSeriesRawResult> query, out StreamQueryStatistics streamQueryStats)
        {
            return StreamTimeSeriesInternal(query, out streamQueryStats);
        }

        public IEnumerator<TimeSeriesStreamResult<TimeSeriesAggregationResult<T>>> Stream<T>(IQueryable<TimeSeriesAggregationResult<T>> query) where T : new()
        {
            return StreamTimeSeriesInternal(query);
        }

        public IEnumerator<TimeSeriesStreamResult<TimeSeriesAggregationResult<T>>> Stream<T>(IQueryable<TimeSeriesAggregationResult<T>> query, out StreamQueryStatistics streamQueryStats) where T : new()
        {
            return StreamTimeSeriesInternal(query, out streamQueryStats);
        }

        public IEnumerator<TimeSeriesStreamResult<TimeSeriesAggregationResult<T>>> Stream<T>(IDocumentQuery<TimeSeriesAggregationResult<T>> query) where T : new()
        {
            return StreamTimeSeriesInternal(query);
        }

        public IEnumerator<TimeSeriesStreamResult<TimeSeriesAggregationResult<T>>> Stream<T>(IRawDocumentQuery<TimeSeriesAggregationResult<T>> query) where T : new()
        {
            return StreamTimeSeriesInternal(query);
        }

        public IEnumerator<TimeSeriesStreamResult<TimeSeriesAggregationResult<T>>> Stream<T>(IRawDocumentQuery<TimeSeriesAggregationResult<T>> query, out StreamQueryStatistics streamQueryStats) where T : new()
        {
            return StreamTimeSeriesInternal(query, out streamQueryStats);
        }

        public IEnumerator<TimeSeriesStreamResult<TimeSeriesAggregationResult<T>>> Stream<T>(IDocumentQuery<TimeSeriesAggregationResult<T>> query, out StreamQueryStatistics streamQueryStats) where T : new()
        {
            return StreamTimeSeriesInternal(query, out streamQueryStats);
        }

        public IEnumerator<TimeSeriesStreamResult<TimeSeriesRawResult<T>>> Stream<T>(IQueryable<TimeSeriesRawResult<T>> query) where T : new()
        {
            return StreamTimeSeriesInternal(query);
        }

        public IEnumerator<TimeSeriesStreamResult<TimeSeriesRawResult<T>>> Stream<T>(IQueryable<TimeSeriesRawResult<T>> query, out StreamQueryStatistics streamQueryStats) where T : new()
        {
            return StreamTimeSeriesInternal(query, out streamQueryStats);
        }

        public IEnumerator<TimeSeriesStreamResult<TimeSeriesRawResult<T>>> Stream<T>(IDocumentQuery<TimeSeriesRawResult<T>> query) where T : new()
        {
            return StreamTimeSeriesInternal(query);
        }

        public IEnumerator<TimeSeriesStreamResult<TimeSeriesRawResult<T>>> Stream<T>(IRawDocumentQuery<TimeSeriesRawResult<T>> query) where T : new()
        {
            return StreamTimeSeriesInternal(query);
        }

        public IEnumerator<TimeSeriesStreamResult<TimeSeriesRawResult<T>>> Stream<T>(IRawDocumentQuery<TimeSeriesRawResult<T>> query, out StreamQueryStatistics streamQueryStats) where T : new()
        {
            return StreamTimeSeriesInternal(query, out streamQueryStats);
        }

        public IEnumerator<TimeSeriesStreamResult<TimeSeriesRawResult<T>>> Stream<T>(IDocumentQuery<TimeSeriesRawResult<T>> query, out StreamQueryStatistics streamQueryStats) where T : new()
        {
            return StreamTimeSeriesInternal(query, out streamQueryStats);
        }
    }
}
