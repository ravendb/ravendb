using System;
using System.Threading;
using Raven.Server.Config.Categories;
using Raven.Server.Documents.Indexes.MapReduce;
using Raven.Server.Documents.Indexes.MapReduce.Static;
using Raven.Server.Documents.Indexes.Persistence;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Indexes.Workers.Cleanup
{
    public sealed class CleanupDocumentsForMapReduce : CleanupDocuments
    {
        private readonly MapReduceIndex _mapReduceIndex;
        private readonly MapReduceIndexingContext _mapReduceContext;

        public CleanupDocumentsForMapReduce(MapReduceIndex mapReduceIndex, DocumentsStorage documentsStorage, IndexStorage indexStorage, IndexingConfiguration configuration, MapReduceIndexingContext mapReduceContext)
            : base(mapReduceIndex, documentsStorage, indexStorage, configuration, mapReduceContext)
        {
            _mapReduceIndex = mapReduceIndex;
            _mapReduceContext = mapReduceContext;
        }

        protected override void WriteLastProcessedTombstoneEtag(RavenTransaction transaction, string collection, long lastEtag) =>
            _mapReduceContext.ProcessedTombstoneEtags[collection] = lastEtag;

        public override (bool MoreWorkFound, Index.CanContinueBatchResult BatchContinuationResult) Execute(QueryOperationContext queryContext, TransactionOperationContext indexContext, Lazy<IndexWriteOperationBase> writeOperation, IndexingStatsScope stats, CancellationToken token)
        {
            var result = base.Execute(queryContext, indexContext, writeOperation, stats, token);

            if (_mapReduceIndex.OutputReduceToCollection?.HasDocumentsToDelete(indexContext) == true)
            {
                result.MoreWorkFound = true;

                if (_mapReduceIndex.IsSideBySide() == false)
                {
                    // we can start deleting reduce output documents only if index becomes a regular one

                    result.MoreWorkFound |= _mapReduceIndex.OutputReduceToCollection.DeleteDocuments(stats, indexContext);
                }
            }

            return result;
        }
    }
}
