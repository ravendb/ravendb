using System.Collections.Generic;
using System.Linq;
using Raven.Server.Config.Categories;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Indexes.Workers
{
    public class HandleCompareExchangeReferences : HandleDocumentReferences
    {
        public HandleCompareExchangeReferences(Index index, DocumentsStorage documentsStorage, IndexStorage indexStorage, IndexingConfiguration configuration)
            : base(index, CreateReferencedCollections(index), documentsStorage, indexStorage, indexStorage.ReferencesForCompareExchange, configuration)
        {
        }

        protected override IEnumerable<Reference> GetItemReferences(DocumentsOperationContext databaseContext, TransactionOperationContext serverContext, CollectionName referencedCollection, long lastEtag, long pageSize)
        {
            return _documentsStorage.DocumentDatabase.ServerStore.Cluster.GetCompareExchangeFromPrefix(serverContext, _documentsStorage.DocumentDatabase.Name, lastEtag + 1, pageSize)
                .Select(x =>
                {
                    _reference.Key = x.Key.StorageKey;
                    _reference.Etag = x.Index;

                    return _reference;
                });
        }

        protected override IEnumerable<Reference> GetTombstoneReferences(DocumentsOperationContext databaseContext, TransactionOperationContext serverContext, CollectionName referencedCollection, long lastEtag, long pageSize)
        {
            return _documentsStorage.DocumentDatabase.ServerStore.Cluster.GetCompareExchangeTombstonesByKey(serverContext, _documentsStorage.DocumentDatabase.Name, lastEtag + 1, pageSize)
                .Select(x =>
                {
                    _reference.Key = x.Key.StorageKey;
                    _reference.Etag = x.Index;

                    return _reference;
                });
        }

        private static Dictionary<string, HashSet<CollectionName>> CreateReferencedCollections(Index index)
        {
            var referencedCollections = new Dictionary<string, HashSet<CollectionName>>();
            foreach (var collection in index.Collections)
            {
                if (referencedCollections.TryGetValue(collection, out HashSet<CollectionName> set) == false)
                    referencedCollections[collection] = set = new HashSet<CollectionName>();

                set.Add(new CollectionName("CompareExchange"));
            }

            return referencedCollections;
        }
    }
}
