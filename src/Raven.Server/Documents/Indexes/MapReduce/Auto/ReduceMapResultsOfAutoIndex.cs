using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using Raven.Client.Documents.Indexes;
using Raven.Server.Json;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Indexes.MapReduce.Auto
{
    public class ReduceMapResultsOfAutoIndex : ReduceMapResultsBase<AutoMapReduceIndexDefinition>
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
