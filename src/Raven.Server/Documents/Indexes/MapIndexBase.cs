using System;
using System.Collections;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Raven.Client.Documents.Indexes;
using Raven.Server.Documents.Includes;
using Raven.Server.Documents.Indexes.Persistence.Lucene;
using Raven.Server.Documents.Indexes.Workers;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.Results;
using Raven.Server.Documents.Queries.Timings;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Indexes
{
    public abstract class MapIndexBase<T, TField> : Index<T, TField> where T : IndexDefinitionBase<TField> where TField : IndexFieldBase
    {
        private CollectionOfBloomFilters _filters;
        private IndexingStatsScope _statsInstance;
        private readonly MapStats _stats = new MapStats();

        protected MapIndexBase(IndexType type, T definition) : base(type, definition)
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

            _filters = CollectionOfBloomFilters.Load(mode, indexContext);

            return _filters;
        }

        public override void HandleDelete(Tombstone tombstone, string collection, IndexWriteOperation writer, TransactionOperationContext indexContext, IndexingStatsScope stats)
        {
            writer.Delete(tombstone.LowerId, stats);
        }

        public override int HandleMap(LazyStringValue lowerId, LazyStringValue id, IEnumerable mapResults, IndexWriteOperation writer, TransactionOperationContext indexContext, IndexingStatsScope stats)
        {
            EnsureValidStats(stats);

            bool mustDelete;
            using (_stats.BloomStats.Start())
            {
                mustDelete = _filters.Add(lowerId) == false;
            }

            if (mustDelete)
                writer.Delete(lowerId, stats);

            var numberOfOutputs = 0;
            foreach (var mapResult in mapResults)
            {
                writer.IndexDocument(lowerId, mapResult, stats, indexContext);
                numberOfOutputs++;
            }

            HandleIndexOutputsPerDocument(id ?? lowerId, numberOfOutputs, stats);

            DocumentDatabase.Metrics.MapIndexes.IndexedPerSec.Mark(numberOfOutputs);

            return numberOfOutputs;
        }

        public override IQueryResultRetriever GetQueryResultRetriever(IndexQueryServerSide query, QueryTimingsScope queryTimings, DocumentsOperationContext documentsContext, FieldsToFetch fieldsToFetch, IncludeDocumentsCommand includeDocumentsCommand)
        {
            return new MapQueryResultRetriever(DocumentDatabase, query, queryTimings, DocumentDatabase.DocumentsStorage, documentsContext, fieldsToFetch, includeDocumentsCommand);
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

        protected override async Task QueryIdsInternal(
            IdsQueryResult resultToFill,
            IndexQueryServerSide query,
            DocumentsOperationContext documentsContext,
            OperationCancelToken token)
        {
            QueryInternalPreparation(query);

            using (var marker = MarkQueryAsRunning(query, token))
            using (var queryContext = await OpenTransactionsForNonStaleResultIfNeeded(query, documentsContext, marker))
            {
                FillQueryResult(resultToFill, queryContext.IsStale, query.Metadata, documentsContext, queryContext.Context);

                using (var reader = IndexPersistence.OpenIndexReader(queryContext.Context.Transaction.InnerTransaction))
                using (var queryScope = query.Timings?.For(nameof(QueryTimingsScope.Names.Query)))
                {
                    var fieldsToFetch = new FieldsToFetch(new SelectField[0], Definition);
                    var retriever = new MapQueryIdsRetriever(DocumentDatabase, query, queryScope, DocumentDatabase.DocumentsStorage, documentsContext, fieldsToFetch);

                    var documents = ReaderQuery(
                        query,
                        documentsContext,
                        token,
                        reader,
                        retriever,
                        fieldsToFetch,
                        queryScope,
                        out var totalResults,
                        out var skippedResults);


                    foreach (var document in documents)
                    {
                        resultToFill.TotalResults = totalResults.Value;
                        resultToFill.AddResult(document.Result.Id);
                    }

                    resultToFill.TotalResults = Math.Max(totalResults.Value, resultToFill.Results.Count);
                    resultToFill.SkippedResults = skippedResults.Value;
                }
            }
        }

        private class MapStats
        {
            public IndexingStatsScope BloomStats;
        }
    }
}
