using Sparrow.Json;
using Sparrow.Logging;

namespace Raven.Server.Documents.Indexes.Persistence
{
    public abstract class IndexWriteOperationBase : IndexOperationBase
    {
        protected IndexWriteOperationBase(Index index, Logger logger) : base(index, logger)
        {
        }

        public abstract void Commit(IndexingStatsScope stats);

        public abstract void Optimize();

        public abstract void IndexDocument(LazyStringValue key, LazyStringValue sourceDocumentId, object document, IndexingStatsScope stats,
            JsonOperationContext indexContext);

        public abstract int EntriesCount();

        public abstract (long RamSizeInBytes, long FilesAllocationsInBytes) GetAllocations();

        public abstract void Delete(LazyStringValue key, IndexingStatsScope stats);

        public abstract void DeleteBySourceDocument(LazyStringValue sourceDocumentId, IndexingStatsScope stats);

        public abstract void DeleteReduceResult(LazyStringValue reduceKeyHash, IndexingStatsScope stats);
    }
}
