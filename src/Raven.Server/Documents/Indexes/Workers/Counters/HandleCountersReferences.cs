using System.Collections.Generic;
using Raven.Server.Documents.Indexes.Persistence.Lucene;
using Raven.Server.ServerWide.Context;
using Sparrow.Server.Utils;
using Voron;

namespace Raven.Server.Documents.Indexes.Workers.Counters
{
    public sealed class HandleCountersReferences : HandleReferences
    {
        private readonly CountersStorage _countersStorage;

        protected override ReferenceType Type => ReferenceType.Counters;

        public HandleCountersReferences(Index index, Dictionary<string, HashSet<CollectionName>> referencedCollections, CountersStorage countersStorage, DocumentsStorage documentsStorage, IndexStorage indexStorage, Config.Categories.IndexingConfiguration configuration)
            : base(index, referencedCollections, documentsStorage, indexStorage, indexStorage.ReferencesForDocuments, configuration)
        {
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
            using (DocumentIdWorker.GetSliceFromId(indexContext, tombstone.LowerId, out Slice documentIdPrefixWithTsKeySeparator, SpecialChars.RecordSeparator))
                _referencesStorage.RemoveReferencesByPrefix(documentIdPrefixWithTsKeySeparator, collection, null, indexContext.Transaction);
        }
    }
}
