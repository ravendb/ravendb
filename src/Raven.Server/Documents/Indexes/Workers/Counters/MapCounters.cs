using System.Collections.Generic;
using Raven.Client;
using Raven.Server.Config.Categories;
using Raven.Server.Documents.Indexes.MapReduce;

namespace Raven.Server.Documents.Indexes.Workers.Counters
{
    public sealed class MapCounters : MapItems
    {
        private readonly CountersStorage _countersStorage;

        public MapCounters(Index index, CountersStorage countersStorage, IndexStorage indexStorage, MapReduceIndexingContext mapReduceContext, IndexingConfiguration configuration)
            : base(index, indexStorage, mapReduceContext, configuration)
        {
            _countersStorage = countersStorage;
        }

        protected override IEnumerable<IndexItem> GetItemsEnumerator(QueryOperationContext queryContext, string collection, long lastEtag, long pageSize)
        {
            foreach (var counter in GetCountersEnumerator(queryContext, collection, lastEtag, pageSize))
                yield return new CounterIndexItem(counter.LuceneKey, counter.DocumentId, counter.Etag, counter.CounterName, counter.Size, counter);
        }

        private IEnumerable<CounterGroupItemMetadata> GetCountersEnumerator(QueryOperationContext queryContext, string collection, long lastEtag, long pageSize)
        {
            if (collection == Constants.Documents.Collections.AllDocumentsCollection)
                return _countersStorage.Indexing.GetCountersMetadataFrom(queryContext.Documents, lastEtag + 1, 0, pageSize);

            return _countersStorage.Indexing.GetCountersMetadataFrom(queryContext.Documents, collection, lastEtag + 1, 0, pageSize);
        }
    }
}
