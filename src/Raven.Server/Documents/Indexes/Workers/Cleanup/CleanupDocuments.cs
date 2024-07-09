using System;
using System.Collections.Generic;
using Raven.Server.Config.Categories;
using Raven.Server.Documents.Indexes.MapReduce;
using Raven.Server.Documents.Indexes.Persistence;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Indexes.Workers.Cleanup
{
    public class CleanupDocuments : CleanupBase
    {
        private readonly Index _index;
        private readonly DocumentsStorage _documentsStorage;

        public CleanupDocuments(Index index, DocumentsStorage documentsStorage, IndexStorage indexStorage,
            IndexingConfiguration configuration, MapReduceIndexingContext mapReduceContext) : base(index, indexStorage, configuration, mapReduceContext)
        {
            _index = index;
            _documentsStorage = documentsStorage;
        }

        public string Name => "DocumentsCleanup";

        protected override IEnumerable<TombstoneIndexItem> GetTombstonesFrom(DocumentsOperationContext context, long etag, long start, long take) =>
            _documentsStorage.GetTombstoneIndexItemsFrom(context, etag, start, take);

        protected override IEnumerable<TombstoneIndexItem> GetTombstonesFrom(DocumentsOperationContext context, string collection, long etag, long start, long take) =>
            _documentsStorage.GetTombstoneIndexItemsFrom(context, collection, etag, start, take);

        protected override bool ValidateType(TombstoneIndexItem tombstone)
        {
            if (tombstone.Type != IndexItemType.Document)
                return false;

            return true;
        }

        protected override bool HandleDelete(TombstoneIndexItem tombstoneIndexItem, string collection, Lazy<IndexWriteOperationBase> writer, QueryOperationContext queryContext,
            TransactionOperationContext indexContext, IndexingStatsScope stats)
        {
            var tombstone = _documentsStorage.GetTombstoneByEtag(queryContext.Documents, tombstoneIndexItem.Etag);
            _index.HandleDelete(tombstone, collection, writer, indexContext, stats);
            return true;
        }
    }
}
