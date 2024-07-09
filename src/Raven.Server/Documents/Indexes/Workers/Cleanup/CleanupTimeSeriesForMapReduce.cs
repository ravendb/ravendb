using System;
using Raven.Server.Config.Categories;
using Raven.Server.Documents.Indexes.MapReduce;
using Raven.Server.Documents.Indexes.Persistence;
using Raven.Server.Documents.Indexes.Static.TimeSeries;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Indexes.Workers.Cleanup
{
    public class CleanupTimeSeriesForMapReduce : CleanupTimeSeries
    {
        private readonly MapReduceTimeSeriesIndex _mapReduceIndex;

        public CleanupTimeSeriesForMapReduce(Index mapReduceIndex, DocumentsStorage documentsStorage, IndexStorage indexStorage,
            IndexingConfiguration configuration, MapReduceIndexingContext mapReduceContext) : base(mapReduceIndex, documentsStorage, indexStorage, configuration, mapReduceContext)
        {
            _mapReduceIndex = mapReduceIndex as MapReduceTimeSeriesIndex;
        }

        protected override void HandleTimeSeriesDelete(TombstoneIndexItem tombstone, string collection, Lazy<IndexWriteOperationBase> writer,
            QueryOperationContext queryContext, TransactionOperationContext indexContext, IndexingStatsScope stats)
        {
            _mapReduceIndex.HandleTimeSeriesDelete(tombstone, indexContext);
            base.HandleTimeSeriesDelete(tombstone, collection, writer, queryContext, indexContext, stats);
        }
    }
}
