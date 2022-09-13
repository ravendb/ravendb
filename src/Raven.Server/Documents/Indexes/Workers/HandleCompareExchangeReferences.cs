using System.Collections.Generic;
using System.Linq;
using Raven.Server.Config.Categories;

namespace Raven.Server.Documents.Indexes.Workers
{
    public class HandleCompareExchangeReferences : HandleDocumentReferences
    {
        private readonly HashSet<string> _collectionsWithCompareExchangeReferences;

        private static readonly HashSet<CollectionName> _referencedCollections = new HashSet<CollectionName>
        {
            IndexStorage.CompareExchangeReferences.CompareExchange
        };

        public HandleCompareExchangeReferences(Index index, HashSet<string> collectionsWithCompareExchangeReferences, DocumentsStorage documentsStorage, IndexStorage indexStorage, IndexingConfiguration configuration)
            : base(index, null, documentsStorage, indexStorage, indexStorage.ReferencesForCompareExchange, configuration)
        {
            _collectionsWithCompareExchangeReferences = collectionsWithCompareExchangeReferences;
        }

        protected override bool ItemsAndReferencesAreUsingSameEtagPool => false;

        protected override IEnumerable<Reference> GetItemReferences(QueryOperationContext queryContext, CollectionName referencedCollection, long lastEtag, long pageSize)
        {
            return _documentsStorage.DocumentDatabase.CompareExchangeStorage.GetCompareExchangeFromPrefix(queryContext.Server, lastEtag + 1, pageSize)
                .Select(x =>
                {
                    _reference.Key = x.Key.StorageKey;
                    _reference.Etag = x.Index;

                    return _reference;
                });
        }

        protected override IEnumerable<Reference> GetTombstoneReferences(QueryOperationContext queryContext, CollectionName referencedCollection, long lastEtag, long pageSize)
        {
            return _documentsStorage.DocumentDatabase.CompareExchangeStorage.GetCompareExchangeTombstonesByKey(queryContext.Server, lastEtag + 1, pageSize)
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

        protected static bool TryGetReferencedCollectionsFor(HashSet<string> collectionsWithCompareExchangeReferences, string collection, out HashSet<CollectionName> referencedCollections)
        {
            if (collectionsWithCompareExchangeReferences.Contains(collection))
            {
                referencedCollections = _referencedCollections;
                return true;
            }

            referencedCollections = null;
            return false;
        }
    }
}
