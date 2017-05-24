using System;
using System.Collections;
using System.Runtime.CompilerServices;
using Raven.Client.Documents.Indexes;
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
        private IndexingStatsScope _statsInstance;
        private readonly MapStats _stats = new MapStats();

        protected MapIndexBase(long etag, IndexType type, T definition) : base(etag, type, definition)
        {
        }

        protected override IIndexingWork[] CreateIndexWorkExecutors()
        {
            return new IIndexingWork[]
            {
                new CleanupDeletedDocuments(this, DocumentDatabase.DocumentsStorage, _indexStorage, Configuration, null),
                new MapDocuments(this, DocumentDatabase.DocumentsStorage, _indexStorage, null, Configuration)
            };
        }

        public override IDisposable InitializeIndexingWork(TransactionOperationContext indexContext)
        {
            var mode = sizeof(int) == IntPtr.Size || DocumentDatabase.Configuration.Storage.ForceUsing32BitsPager
                ? CollectionOfBloomFilters.Mode.X86
                : CollectionOfBloomFilters.Mode.X64;

            _filter = CollectionOfBloomFilters.Load(mode, indexContext);

            return _filter;
        }

        public override void HandleDelete(DocumentTombstone tombstone, string collection, IndexWriteOperation writer, TransactionOperationContext indexContext, IndexingStatsScope stats)
        {
            writer.Delete(tombstone.LowerId, stats);
        }

        public override int HandleMap(LazyStringValue key, IEnumerable mapResults, IndexWriteOperation writer, TransactionOperationContext indexContext, IndexingStatsScope stats)
        {
            EnsureValidStats(stats);

            bool mustDelete;
            using (_stats.BloomStats.Start())
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
            }

            HandleIndexOutputsPerDocument(key, numberOfOutputs, stats);

            DocumentDatabase.Metrics.IndexedPerSecond.Mark();
            return numberOfOutputs;
        }

        public override IQueryResultRetriever GetQueryResultRetriever(DocumentsOperationContext documentsContext, FieldsToFetch fieldsToFetch)
        {
            return new MapQueryResultRetriever(DocumentDatabase.DocumentsStorage, documentsContext, fieldsToFetch);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void EnsureValidStats(IndexingStatsScope stats)
        {
            if (_statsInstance == stats)
                return;

            _statsInstance = stats;
            _stats.BloomStats = stats.For(IndexingOperation.Map.Bloom, start: false);
        }

        private class MapStats
        {
            public IndexingStatsScope BloomStats;
        }
    }
}