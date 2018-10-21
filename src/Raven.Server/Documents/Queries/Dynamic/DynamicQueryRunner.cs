using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Queries;
using Raven.Client.Exceptions.Documents.Indexes;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Indexes.Auto;
using Raven.Server.Documents.Queries.Suggestions;
using Raven.Server.Documents.Queries.Timings;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Json;
using Sparrow.Utils;
using PatchRequest = Raven.Server.Documents.Patch.PatchRequest;

namespace Raven.Server.Documents.Queries.Dynamic
{
    public class DynamicQueryRunner : AbstractQueryRunner
    {
        private readonly IndexStore _indexStore;

        public DynamicQueryRunner(DocumentDatabase database) : base(database)
        {
            _indexStore = database.IndexStore;
        }

        public override async Task ExecuteStreamQuery(IndexQueryServerSide query, DocumentsOperationContext documentsContext, HttpResponse response, IStreamDocumentQueryResultWriter writer,
            OperationCancelToken token)
        {
            var index = await MatchIndex(query, true, customStalenessWaitTimeout: TimeSpan.FromSeconds(60), documentsContext, token.Token);

            await index.StreamQuery(response, writer, query, documentsContext, token);
        }

        public override async Task<DocumentQueryResult> ExecuteQuery(IndexQueryServerSide query, DocumentsOperationContext documentsContext, long? existingResultEtag, OperationCancelToken token)
        {
            Index index;
            using (query.Timings?.For(nameof(QueryTimingsScope.Names.Optimizer)))
                index = await MatchIndex(query, true, null, documentsContext, token.Token);

            if (query.Metadata.HasOrderByRandom == false && existingResultEtag.HasValue)
            {
                var etag = index.GetIndexEtag(query.Metadata);
                if (etag == existingResultEtag)
                    return DocumentQueryResult.NotModifiedResult;
            }

            return await index.Query(query, documentsContext, token);
        }

        public override async Task<IndexEntriesQueryResult> ExecuteIndexEntriesQuery(IndexQueryServerSide query, DocumentsOperationContext context, long? existingResultEtag, OperationCancelToken token)
        {
            var index = await MatchIndex(query, false, null, context, token.Token);

            if (index == null)
                IndexDoesNotExistException.ThrowFor(query.Metadata.CollectionName);

            if (existingResultEtag.HasValue)
            {
                var etag = index.GetIndexEtag(query.Metadata);
                if (etag == existingResultEtag)
                    return IndexEntriesQueryResult.NotModifiedResult;
            }

            return index.IndexEntries(query, context, token);
        }

        public override async Task<IOperationResult> ExecuteDeleteQuery(IndexQueryServerSide query, QueryOperationOptions options, DocumentsOperationContext context, Action<IOperationProgress> onProgress, OperationCancelToken token)
        {
            var index = await MatchIndex(query, true, null, context, token.Token);

            return await ExecuteDelete(query, index, options, context, onProgress, token);
        }

        public override async Task<IOperationResult> ExecutePatchQuery(IndexQueryServerSide query, QueryOperationOptions options, PatchRequest patch, BlittableJsonReaderObject patchArgs, DocumentsOperationContext context, Action<IOperationProgress> onProgress, OperationCancelToken token)
        {
            var index = await MatchIndex(query, true, null, context, token.Token);

            return await ExecutePatch(query, index, options, patch, patchArgs, context, onProgress, token);
        }

        public override async Task<SuggestionQueryResult> ExecuteSuggestionQuery(IndexQueryServerSide query, DocumentsOperationContext documentsContext, long? existingResultEtag, OperationCancelToken token)
        {
            var index = await MatchIndex(query, true, null, documentsContext, token.Token);

            return await ExecuteSuggestion(query, index, documentsContext, existingResultEtag, token);
        }

        private async Task<Index> MatchIndex(IndexQueryServerSide query, bool createAutoIndexIfNoMatchIsFound, TimeSpan? customStalenessWaitTimeout, DocumentsOperationContext docsContext,
            CancellationToken token)
        {
            Index index;
            if (query.Metadata.AutoIndexName != null)
            {
                index = _indexStore.GetIndex(query.Metadata.AutoIndexName);

                if (index != null)
                    return index;
            }

            var map = DynamicQueryMapping.Create(query);

            if (TryMatchExistingIndexToQuery(map, docsContext, out index) == false)
            {
                if (createAutoIndexIfNoMatchIsFound == false)
                    throw new IndexDoesNotExistException("Could not find index for a given query.");

                var definition = map.CreateAutoIndexDefinition();

                index = await _indexStore.CreateIndex(definition);

                if (query.WaitForNonStaleResultsTimeout.HasValue == false)
                {
                    if (customStalenessWaitTimeout.HasValue)
                        query.WaitForNonStaleResultsTimeout = customStalenessWaitTimeout.Value;
                    else
                        query.WaitForNonStaleResultsTimeout = TimeSpan.FromSeconds(15); // allow new auto indexes to have some results
                }

                var t = CleanupSupersededAutoIndexes(index, map, token)
                    .ContinueWith(task =>
                    {
                        if (task.Exception != null)
                        {
                            if (token.IsCancellationRequested)
                                return;

                            if (_indexStore.Logger.IsInfoEnabled)
                            {
                                _indexStore.Logger.Info("Failed to delete superseded indexes for index " + index.Name);
                            }
                        }
                    });

                if (query.WaitForNonStaleResults &&
                    Database.Configuration.Indexing.TimeToWaitBeforeDeletingAutoIndexMarkedAsIdle.AsTimeSpan ==
                    TimeSpan.Zero)
                    await t; // this is used in testing, mainly
            }

            return index;
        }

        private async Task CleanupSupersededAutoIndexes(Index index, DynamicQueryMapping map, CancellationToken token)
        {
            if (map.SupersededIndexes == null || map.SupersededIndexes.Count == 0)
                return;

            // this is meant to remove superseded indexes immediately when they are of no use
            // however, they'll also be cleaned by the idle timer, so we don't worry too much
            // about this being in memory only operation

            while (token.IsCancellationRequested == false)
            {
                AsyncManualResetEvent.FrozenAwaiter indexingBatchCompleted;
                try
                {
                    indexingBatchCompleted = index.GetIndexingBatchAwaiter();
                }
                catch (ObjectDisposedException)
                {
                    break;
                }

                var maxSupersededEtag = 0L;
                foreach (var supersededIndex in map.SupersededIndexes)
                {
                    try
                    {
                        var etag = supersededIndex.GetLastMappedEtagFor(map.ForCollection);
                        maxSupersededEtag = Math.Max(etag, maxSupersededEtag);
                    }
                    catch (OperationCanceledException)
                    {
                        // the superseded index was already deleted
                    }
                }

                long currentEtag;
                try
                {
                    currentEtag = index.GetLastMappedEtagFor(map.ForCollection);
                }
                catch (OperationCanceledException)
                {
                    // the index was already disposed by something else
                    break;
                }
                if (currentEtag >= maxSupersededEtag)
                {
                    // we'll give it a few seconds to drain any pending queries,
                    // and because it make it easier to demonstrate how we auto
                    // clear the old auto indexes.
                    var timeout = Database.Configuration.Indexing.TimeBeforeDeletionOfSupersededAutoIndex.AsTimeSpan;
                    if (timeout != TimeSpan.Zero)
                    {
                        await TimeoutManager.WaitFor(
                            timeout
                        ).ConfigureAwait(false);
                    }

                    foreach (var supersededIndex in map.SupersededIndexes)
                    {
                        try
                        {
                            await _indexStore.DeleteIndex(supersededIndex.Name);
                        }
                        catch (IndexDoesNotExistException)
                        {
                        }
                    }
                    break;
                }

                if (await indexingBatchCompleted.WaitAsync() == false)
                    break;
            }
        }

        public List<DynamicQueryToIndexMatcher.Explanation> ExplainIndexSelection(IndexQueryServerSide query, DocumentsOperationContext docsContext, out string indexName)
        {
            var map = DynamicQueryMapping.Create(query);
            var explanations = new List<DynamicQueryToIndexMatcher.Explanation>();

            var dynamicQueryToIndex = new DynamicQueryToIndexMatcher(_indexStore);
            var match = dynamicQueryToIndex.Match(map, docsContext, explanations);
            indexName = match.IndexName;

            return explanations;
        }

        private bool TryMatchExistingIndexToQuery(DynamicQueryMapping map, DocumentsOperationContext docsContext, out Index index)
        {
            var dynamicQueryToIndex = new DynamicQueryToIndexMatcher(_indexStore);

            var matchResult = dynamicQueryToIndex.Match(map, docsContext);

            switch (matchResult.MatchType)
            {
                case DynamicQueryMatchType.Complete:
                case DynamicQueryMatchType.CompleteButIdle:
                    index = _indexStore.GetIndex(matchResult.IndexName);
                    return true;
                case DynamicQueryMatchType.Partial:
                    // At this point, we found an index that has some fields we need and
                    // isn't incompatible with anything else we're asking for
                    // We need to clone that other index 
                    // We need to add all our requested indexes information to our cloned index
                    // We can then use our new index instead

                    var currentIndex = _indexStore.GetIndex(matchResult.IndexName);

                    if (map.SupersededIndexes == null)
                        map.SupersededIndexes = new List<Index>();

                    map.SupersededIndexes.Add(currentIndex);

                    map.ExtendMappingBasedOn((AutoIndexDefinitionBase)currentIndex.Definition);

                    break;
            }

            index = null;
            return false;
        }
    }
}
