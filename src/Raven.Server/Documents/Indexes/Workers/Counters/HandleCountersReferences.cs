using System.Collections.Generic;
using Raven.Server.Documents.Indexes.Persistence.Lucene;
using Raven.Server.ServerWide.Context;
using Voron;

namespace Raven.Server.Documents.Indexes.Workers.Counters
{
    public sealed class HandleCountersReferences : HandleReferences
    {
        private readonly CountersStorage _countersStorage;

        public HandleCountersReferences(Index index, Dictionary<string, HashSet<CollectionName>> referencedCollections, CountersStorage countersStorage, DocumentsStorage documentsStorage, IndexStorage indexStorage, Config.Categories.IndexingConfiguration configuration)
            : base(index, referencedCollections, documentsStorage, indexStorage, indexStorage.ReferencesForDocuments, configuration)
        {
            _countersStorage = countersStorage;
        }

        protected override IEnumerable<IndexItem> GetItems(DocumentsOperationContext databaseContext, Slice key)
        {
            foreach (var counter in _countersStorage.GetCountersMetadata(databaseContext, key))
            {
                foreach (var counterName in counter.CounterNames)
                    yield return new CounterIndexItem(counter.DocumentId, counter.DocumentId, counter.Etag, counterName, counter.Size);
            }
        }

        public override void HandleDelete(Tombstone tombstone, string collection, IndexWriteOperation writer, TransactionOperationContext indexContext, IndexingStatsScope stats)
        {
            var tx = indexContext.Transaction.InnerTransaction;

            using (Slice.External(tx.Allocator, tombstone.LowerId, out Slice tombstoneKeySlice))
                _referencesStorage.RemoveReferences(tombstoneKeySlice, collection, null, indexContext.Transaction);
        }
    }
}
