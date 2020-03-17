using System;
using System.Collections.Generic;
using Raven.Server.Documents.Indexes.Persistence.Lucene;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Server.Utils;
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
            using (_countersStorage.Indexing.ExtractDocumentIdFromKey(databaseContext, key, out var documentId))
            {
                foreach (var counter in _countersStorage.Indexing.GetCountersMetadata(databaseContext, documentId))
                    yield return new CounterIndexItem(counter.Key, counter.DocumentId, counter.Etag, counter.CounterName, counter.Size, counter);
            }
        }

        public override void HandleDelete(Tombstone tombstone, string collection, IndexWriteOperation writer, TransactionOperationContext indexContext, IndexingStatsScope stats)
        {
            using (DocumentIdWorker.GetSliceFromId(indexContext, tombstone.LowerId, out Slice documentIdPrefixWithTsKeySeparator, SpecialChars.RecordSeparator))
                _referencesStorage.RemoveReferencesByPrefix(documentIdPrefixWithTsKeySeparator, collection, null, indexContext.Transaction);
        }
    }
}
