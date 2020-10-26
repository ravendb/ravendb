using System.Collections.Generic;
using System.Linq;
using Raven.Server.Config.Categories;
using Raven.Server.Documents.Indexes.Persistence.Lucene;
using Raven.Server.ServerWide.Context;
using Sparrow.Server.Utils;
using Voron;

namespace Raven.Server.Documents.Indexes.Workers.Counters
{
    public class HandleCompareExchangeCountersReferences : HandleCompareExchangeReferences
    {
        private readonly HashSet<string> _collectionsWithCompareExchangeReferences;
        private readonly CountersStorage _countersStorage;

        protected override ReferenceType Type => ReferenceType.CompareExchangeCounters;

        public HandleCompareExchangeCountersReferences(Index index, HashSet<string> collectionsWithCompareExchangeReferences, CountersStorage countersStorage, DocumentsStorage documentsStorage, IndexStorage indexStorage, IndexingConfiguration configuration)
            : base(index, collectionsWithCompareExchangeReferences, documentsStorage, indexStorage, configuration)
        {
            _collectionsWithCompareExchangeReferences = collectionsWithCompareExchangeReferences;
            _countersStorage = countersStorage;
        }

        protected override IndexItem GetItem(DocumentsOperationContext databaseContext, Slice key)
        {
            var counter = _countersStorage.Indexing.GetCountersMetadata(databaseContext, key);

            if (counter == null)
                return null;

            return new CounterIndexItem(counter.LuceneKey, counter.DocumentId, counter.Etag, counter.CounterName, counter.Size, counter);
        }

        public override void HandleDelete(Tombstone tombstone, string collection, IndexWriteOperation writer, TransactionOperationContext indexContext, IndexingStatsScope stats)
        {
            using (DocumentIdWorker.GetSliceFromId(indexContext, tombstone.LowerId, out Slice documentIdPrefixWithCounterKeySeparator, SpecialChars.RecordSeparator))
                _referencesStorage.RemoveReferencesByPrefix(documentIdPrefixWithCounterKeySeparator, collection, null, indexContext.Transaction);
        }

        protected override IEnumerable<Reference> GetItemReferences(QueryOperationContext queryContext, CollectionName referencedCollection, long lastEtag, long pageSize)
        {
            return _documentsStorage.DocumentDatabase.ServerStore.Cluster.GetCompareExchangeFromPrefix(queryContext.Server, _documentsStorage.DocumentDatabase.Name, lastEtag + 1, pageSize)
                .Select(x =>
                {
                    _reference.Key = x.Key.StorageKey;
                    _reference.Etag = x.Index;

                    return _reference;
                });
        }

        protected override IEnumerable<Reference> GetTombstoneReferences(QueryOperationContext queryContext, CollectionName referencedCollection, long lastEtag, long pageSize)
        {
            return _documentsStorage.DocumentDatabase.ServerStore.Cluster.GetCompareExchangeTombstonesByKey(queryContext.Server, _documentsStorage.DocumentDatabase.Name, lastEtag + 1, pageSize)
                .Select(x =>
                {
                    _reference.Key = x.Key.StorageKey;
                    _reference.Etag = x.Index;

                    return _reference;
                });
        }

        protected override bool TryGetReferencedCollectionsFor(string collection, out HashSet<CollectionName> referencedCollections)
        {
            return TryGetReferencedCollectionsFor(_collectionsWithCompareExchangeReferences, collection, out referencedCollections);
        }
    }
}
