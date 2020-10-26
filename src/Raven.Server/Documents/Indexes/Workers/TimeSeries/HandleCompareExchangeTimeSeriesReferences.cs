using System.Collections.Generic;
using System.Linq;
using Raven.Server.Config.Categories;
using Raven.Server.Documents.Indexes.Persistence.Lucene;
using Raven.Server.Documents.TimeSeries;
using Raven.Server.ServerWide.Context;
using Sparrow.Server.Utils;
using Voron;

namespace Raven.Server.Documents.Indexes.Workers.TimeSeries
{
    public class HandleCompareExchangeTimeSeriesReferences : HandleCompareExchangeReferences
    {
        private readonly HashSet<string> _collectionsWithCompareExchangeReferences;
        private readonly TimeSeriesStorage _timeSeriesStorage;

        public HandleCompareExchangeTimeSeriesReferences(Index index, HashSet<string> collectionsWithCompareExchangeReferences, TimeSeriesStorage timeSeriesStorage, DocumentsStorage documentsStorage, IndexStorage indexStorage, IndexingConfiguration configuration)
            : base(index, collectionsWithCompareExchangeReferences, documentsStorage, indexStorage, configuration)
        {
            _collectionsWithCompareExchangeReferences = collectionsWithCompareExchangeReferences;
            _timeSeriesStorage = timeSeriesStorage;
        }

        protected override IndexItem GetItem(DocumentsOperationContext databaseContext, Slice key)
        {
            var timeSeries = _timeSeriesStorage.GetTimeSeries(databaseContext, key);
            if (timeSeries == null)
                return null;

            return new TimeSeriesIndexItem(timeSeries.LuceneKey, timeSeries.DocId, timeSeries.Etag, default, timeSeries.Name, timeSeries.SegmentSize, timeSeries);
        }

        public override void HandleDelete(Tombstone tombstone, string collection, IndexWriteOperation writer, TransactionOperationContext indexContext, IndexingStatsScope stats)
        {
            using (DocumentIdWorker.GetSliceFromId(indexContext, tombstone.LowerId, out Slice documentIdPrefixWithTsKeySeparator, SpecialChars.RecordSeparator))
                _referencesStorage.RemoveReferencesByPrefix(documentIdPrefixWithTsKeySeparator, collection, null, indexContext.Transaction);
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
