using System;
using System.Collections;
using System.Runtime.CompilerServices;
using Raven.Client.Documents.Indexes;
using Raven.Server.Documents.Includes;
using Raven.Server.Documents.Indexes.Persistence.Lucene;
using Raven.Server.Documents.Indexes.Workers;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.Results;
using Raven.Server.Documents.Queries.Timings;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Indexes
{
    public abstract class MapIndexBase<T, TField> : Index<T, TField> where T : IndexDefinitionBaseServerSide<TField> where TField : IndexFieldBase
    {
        private CollectionOfBloomFilters _filters;
        private IndexingStatsScope _statsInstance;
        private readonly MapStats _stats = new MapStats();

        protected MapIndexBase(IndexType type, IndexSourceType sourceType, T definition) : base(type, sourceType, definition)
        {
        }

        protected override IIndexingWork[] CreateIndexWorkExecutors()
        {
            return new IIndexingWork[]
            {
                new CleanupDocuments(this, DocumentDatabase.DocumentsStorage, _indexStorage, Configuration, null),
                new MapDocuments(this, DocumentDatabase.DocumentsStorage, _indexStorage, null, Configuration)
            };
        }

        public override IDisposable InitializeIndexingWork(TransactionOperationContext indexContext)
        {
            var mode = DocumentDatabase.Is32Bits
                ? CollectionOfBloomFilters.Mode.X86
                : CollectionOfBloomFilters.Mode.X64;

            if (_filters == null || _filters.Consumed == false)
                _filters = CollectionOfBloomFilters.Load(mode, indexContext);

            return _filters;
        }

        public override void HandleDelete(Tombstone tombstone, string collection, IndexWriteOperation writer, TransactionOperationContext indexContext, IndexingStatsScope stats)
        {
            writer.Delete(tombstone.LowerId, stats);
        }

        public override int HandleMap(IndexItem indexItem, IEnumerable mapResults, IndexWriteOperation writer, TransactionOperationContext indexContext, IndexingStatsScope stats)
        {
            EnsureValidStats(stats);

            bool mustDelete;
            using (_stats.BloomStats.Start())
            {
                mustDelete = _filters.Add(indexItem.LowerId) == false;
            }

            if (indexItem.SkipLuceneDelete == false && mustDelete)
                writer.Delete(indexItem.LowerId, stats);

            var numberOfOutputs = 0;
            foreach (var mapResult in mapResults)
            {
                writer.IndexDocument(indexItem.LowerId, indexItem.LowerSourceDocumentId, mapResult, stats, indexContext);

                numberOfOutputs++;
            }

            HandleIndexOutputsPerDocument(indexItem.Id ?? indexItem.LowerId, numberOfOutputs, stats);

            DocumentDatabase.Metrics.MapIndexes.IndexedPerSec.Mark(numberOfOutputs);

            return numberOfOutputs;
        }

        public override IQueryResultRetriever GetQueryResultRetriever(IndexQueryServerSide query, QueryTimingsScope queryTimings, DocumentsOperationContext documentsContext, FieldsToFetch fieldsToFetch, IncludeDocumentsCommand includeDocumentsCommand, IncludeCompareExchangeValuesCommand includeCompareExchangeValuesCommand)
        {
            return new MapQueryResultRetriever(DocumentDatabase, query, queryTimings, DocumentDatabase.DocumentsStorage, documentsContext, fieldsToFetch, includeDocumentsCommand, includeCompareExchangeValuesCommand);
        }

        public override void SaveLastState()
        {
            _filters?.Flush();
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
