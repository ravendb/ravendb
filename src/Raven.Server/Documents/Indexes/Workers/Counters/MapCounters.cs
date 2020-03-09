using System.Collections.Generic;
using Raven.Client;
using Raven.Client.Documents.Operations.Counters;
using Raven.Server.Config.Categories;
using Raven.Server.Documents.Indexes.MapReduce;
using Sparrow.Json;

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
            foreach (var counters in GetCountersEnumerator(queryContext, collection, lastEtag, pageSize))
            {
                foreach (var counterName in counters.CounterNames)
                    yield return new CounterIndexItem(counters.DocumentId, counters.DocumentId, counters.Etag, counterName, counters.Size);
            }
        }

        private IEnumerable<CounterGroupMetadata> GetCountersEnumerator(QueryOperationContext queryContext, string collection, long lastEtag, long pageSize)
        {
            if (collection == Constants.Documents.Collections.AllDocumentsCollection)
                return _countersStorage.GetCountersMetadataFrom(queryContext.Documents, lastEtag + 1, 0, pageSize);

            return _countersStorage.GetCountersMetadataFrom(queryContext.Documents, collection, lastEtag + 1, 0, pageSize);
        }
    }
}
