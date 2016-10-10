using System;
using System.Collections;
using System.Runtime.CompilerServices;
using Raven.Client.Data.Indexes;
using Raven.Server.Documents.Indexes.Persistence.Lucene;
using Raven.Server.Documents.Indexes.Workers;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.Results;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Indexes
{
    public abstract class MapIndexBase<T> : Index<T> where T : IndexDefinitionBase
    {
        private CollectionOfBloomFilters _filter;
        private IndexingStatsScope _stats;
        private IndexingStatsScope _bloomStats;

        protected MapIndexBase(int indexId, IndexType type, T definition) : base(indexId, type, definition)
        {
        }

        protected override IIndexingWork[] CreateIndexWorkExecutors()
        {
            return new IIndexingWork[]
            {
                new CleanupDeletedDocuments(this, DocumentDatabase.DocumentsStorage, _indexStorage, DocumentDatabase.Configuration.Indexing, null),
                new MapDocuments(this, DocumentDatabase.DocumentsStorage, _indexStorage, null, DocumentDatabase.Configuration.Indexing)
            };
        }

        public override IDisposable InitializeIndexingWork(TransactionOperationContext indexContext)
        {
            _filter = CollectionOfBloomFilters.Load(CollectionOfBloomFilters.BloomFilter.Capacity, indexContext);

            return null;
        }

        public override void HandleDelete(DocumentTombstone tombstone, string collection, IndexWriteOperation writer, TransactionOperationContext indexContext, IndexingStatsScope stats)
        {
            writer.Delete(tombstone.LoweredKey, stats);
        }

        public override int HandleMap(LazyStringValue key, IEnumerable mapResults, IndexWriteOperation writer, TransactionOperationContext indexContext, IndexingStatsScope stats)
        {
            EnsureValidStats(stats);

            bool mustDelete;
            using (_bloomStats?.Start() ?? (_bloomStats = stats.For(IndexingOperation.Map.Bloom)))
            {
                mustDelete = _filter.Add(key) == false;
            }

            if (mustDelete)
                writer.Delete(key, stats);

            var numberOfOutputs = 0;
            foreach (var mapResult in mapResults)
            {
                writer.IndexDocument(key, mapResult, stats, indexContext);
                numberOfOutputs++;

                if (EnsureValidNumberOfOutputsForDocument(numberOfOutputs))
                    continue;

                writer.Delete(key, stats); // TODO [ppekrol] we want to delete invalid doc from index?

                throw new InvalidOperationException($"Index '{Name}' has already produced {numberOfOutputs} map results for a source document '{key}', while the allowed max number of outputs is {MaxNumberOfIndexOutputs} per one document. Please verify this index definition and consider a re-design of your entities or index.");
            }

            DocumentDatabase.Metrics.IndexedPerSecond.Mark();
            return numberOfOutputs;
        }

        public override IQueryResultRetriever GetQueryResultRetriever(DocumentsOperationContext documentsContext, TransactionOperationContext indexContext, FieldsToFetch fieldsToFetch)
        {
            return new MapQueryResultRetriever(DocumentDatabase.DocumentsStorage, documentsContext, fieldsToFetch);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void EnsureValidStats(IndexingStatsScope stats)
        {
            if (_stats == stats)
                return;

            _stats = stats;
            _bloomStats = null;
        }
    }
}