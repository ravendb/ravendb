using System.Collections.Generic;
using System.Threading;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;

namespace Raven.Server.Documents.Indexes.MapReduce.Auto
{
    internal sealed class ReduceMapResultsOfAutoIndex : ReduceMapResultsBase<AutoMapReduceIndexDefinition>
    {
        public static readonly AutoMapReduceIndexResultsAggregator Aggregator = new();

        private BlittableJsonReaderObject _currentlyProcessedResult;

        public ReduceMapResultsOfAutoIndex(Index index, AutoMapReduceIndexDefinition indexDefinition, IndexStorage indexStorage, MetricCounters metrics, MapReduceIndexingContext mapReduceContext)
            : base(index, indexDefinition, indexStorage, metrics, mapReduceContext)
        {
        }

        protected override BlittableJsonReaderObject CurrentlyProcessedResult => _currentlyProcessedResult;

        protected override AggregationResult AggregateOnImpl(List<BlittableJsonReaderObject> aggregationBatch, TransactionOperationContext indexContext, IndexingStatsScope stats, CancellationToken token)
        {
            return Aggregator.AggregateOn(aggregationBatch, _indexDefinition, indexContext, stats, ref _currentlyProcessedResult, token);
        }
    }
}
