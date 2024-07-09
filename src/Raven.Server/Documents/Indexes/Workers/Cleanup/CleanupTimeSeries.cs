using System;
using System.Collections.Generic;
using Raven.Server.Config.Categories;
using Raven.Server.Documents.Indexes.MapReduce;
using Raven.Server.Documents.Indexes.Persistence;
using Raven.Server.Documents.TimeSeries;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Indexes.Workers.Cleanup
{
    public class CleanupTimeSeries : CleanupBase
    {
        private readonly TimeSeriesStorage _tsStorage;
       
        public CleanupTimeSeries(Index index, DocumentsStorage documentsStorage, IndexStorage indexStorage,
            IndexingConfiguration configuration, MapReduceIndexingContext mapReduceContext) : base (index, indexStorage, configuration, mapReduceContext)
        {
            _tsStorage = documentsStorage.TimeSeriesStorage;
        }

        public string Name => "TimeSeriesCleanup";

        protected override IEnumerable<TombstoneIndexItem> GetTombstonesFrom(DocumentsOperationContext context, long etag, long start, long take) =>
            _tsStorage.GetTimeSeriesDeletedRangeIndexItemsFrom(context, etag, take);
       

        protected override IEnumerable<TombstoneIndexItem> GetTombstonesFrom(DocumentsOperationContext context, string collection, long etag, long start, long take) =>
            _tsStorage.GetTimeSeriesDeletedRangeIndexItemsFrom(context, collection, etag, take);

        protected override bool ValidateType(TombstoneIndexItem tombstone)
        {
            if (tombstone.Type != IndexItemType.TimeSeries)
                return false;

            return true;
        }

        protected override bool HandleDelete(TombstoneIndexItem tombstone, string collection, Lazy<IndexWriteOperationBase> writer, 
            QueryOperationContext queryContext, TransactionOperationContext indexContext,
            IndexingStatsScope stats)
        {
            var tsStats = _tsStorage.Stats.GetStats(queryContext.Documents, tombstone.LowerId, tombstone.Name);
            if (tsStats == default || tsStats.Count == 0)
            {
                HandleTimeSeriesDelete(tombstone, collection, writer, queryContext, indexContext, stats);
                return true;
            }

            return false;
        }

        protected virtual void HandleTimeSeriesDelete(TombstoneIndexItem tombstone, string collection, Lazy<IndexWriteOperationBase> writer,
            QueryOperationContext queryContext, TransactionOperationContext indexContext, IndexingStatsScope stats)
        {
            writer.Value.DeleteTimeSeries(tombstone.LowerId, tombstone.LuceneKey, stats);
        }
    }
}
