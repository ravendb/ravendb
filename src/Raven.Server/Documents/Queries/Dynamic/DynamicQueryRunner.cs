using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Queries;
using Raven.Client.Exceptions.Documents.Indexes;
using Raven.Client.Util;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Indexes.Auto;
using Raven.Server.Documents.Queries.Suggestions;
using Raven.Server.Documents.Queries.Timings;
using Raven.Server.ServerWide;
using Sparrow.Json;
using Sparrow.Server;
using Sparrow.Utils;
using Index = Raven.Server.Documents.Indexes.Index;
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

        public override async Task ExecuteStreamQuery(IndexQueryServerSide query, QueryOperationContext queryContext, HttpResponse response, IStreamQueryResultWriter<Document> writer,
            OperationCancelToken token)
        {
            var index = await MatchIndex(query, true, customStalenessWaitTimeout: TimeSpan.FromSeconds(60), token.Token);

            queryContext.WithIndex(index);

            using (QueryRunner.MarkQueryAsRunning(index.Name, query, token, isStreaming: true))
            {
                await index.StreamQuery(response, writer, query, queryContext, token);
            }
        }

        public override async Task<DocumentQueryResult> ExecuteQuery(IndexQueryServerSide query, QueryOperationContext queryContext, long? existingResultEtag, OperationCancelToken token)
        {
            Index index;
            using (query.Timings?.For(nameof(QueryTimingsScope.Names.Optimizer)))
                index = await MatchIndex(query, true, null, token.Token);

            queryContext.WithIndex(index);

            if (query.Metadata.HasOrderByRandom == false && existingResultEtag.HasValue)
            {
                var etag = index.GetIndexEtag(queryContext, query.Metadata);
                if (etag == existingResultEtag)
                    return DocumentQueryResult.NotModifiedResult;
            }

            using (QueryRunner.MarkQueryAsRunning(index.Name, query, token))
            {
                return await index.Query(query, queryContext, token);
            }
        }

        public override async Task<IndexEntriesQueryResult> ExecuteIndexEntriesQuery(IndexQueryServerSide query, QueryOperationContext queryContext, long? existingResultEtag, OperationCancelToken token)
        {
            var index = await MatchIndex(query, false, null, token.Token);

            if (index == null)
                IndexDoesNotExistException.ThrowFor(query.Metadata.CollectionName);

            queryContext.WithIndex(index);

            if (existingResultEtag.HasValue)
            {
                var etag = index.GetIndexEtag(queryContext, query.Metadata);
                if (etag == existingResultEtag)
                    return IndexEntriesQueryResult.NotModifiedResult;
            }

            using (QueryRunner.MarkQueryAsRunning(index.Name, query, token))
            {
                return await index.IndexEntries(query, queryContext, token);
            }
        }

        public override Task ExecuteStreamIndexEntriesQuery(IndexQueryServerSide query, QueryOperationContext queryContext, HttpResponse response,
            IStreamQueryResultWriter<BlittableJsonReaderObject> writer, OperationCancelToken token)
        {
            throw new NotSupportedException("Collection query is handled directly by documents storage so index entries aren't created underneath");
        }

        public override async Task<IOperationResult> ExecuteDeleteQuery(IndexQueryServerSide query, QueryOperationOptions options, QueryOperationContext queryContext, Action<IOperationProgress> onProgress, OperationCancelToken token)
        {
            var index = await MatchIndex(query, true, null, token.Token);

            queryContext.WithIndex(index);

            using (QueryRunner.MarkQueryAsRunning(index.Name, query, token))
            {
                return await ExecuteDelete(query, index, options, queryContext, onProgress, token);
            }
        }

        public override async Task<IOperationResult> ExecutePatchQuery(IndexQueryServerSide query, QueryOperationOptions options, PatchRequest patch, BlittableJsonReaderObject patchArgs, QueryOperationContext queryContext, Action<IOperationProgress> onProgress, OperationCancelToken token)
        {
            var index = await MatchIndex(query, true, null, token.Token);

            queryContext.WithIndex(index);

            using (QueryRunner.MarkQueryAsRunning(index.Name, query, token))
            {
                return await ExecutePatch(query, index, options, patch, patchArgs, queryContext, onProgress, token);
            }
        }

        public override async Task<SuggestionQueryResult> ExecuteSuggestionQuery(IndexQueryServerSide query, QueryOperationContext queryContext, long? existingResultEtag, OperationCancelToken token)
        {
            var index = await MatchIndex(query, true, null, token.Token);

            queryContext.WithIndex(index);

            using (QueryRunner.MarkQueryAsRunning(index.Name, query, token))
            {
                return await ExecuteSuggestion(query, index, queryContext, existingResultEtag, token);
            }
        }

        public async Task<Index> MatchIndex(IndexQueryServerSide query, bool createAutoIndexIfNoMatchIsFound, TimeSpan? customStalenessWaitTimeout, CancellationToken token)
        {
            Index index;
            if (query.Metadata.AutoIndexName != null)
            {
                index = GetIndex(query.Metadata.AutoIndexName, throwIfNotExists: false);

                if (index != null)
                    return index;
            }

            (index, _) = await CreateAutoIndexIfNeeded(query, createAutoIndexIfNoMatchIsFound, customStalenessWaitTimeout, token);

            return index;
        }

        public async Task<(Index Index, bool HasCreatedAutoIndex)> CreateAutoIndexIfNeeded(IndexQueryServerSide query, bool createAutoIndexIfNoMatchIsFound, TimeSpan? customStalenessWaitTimeout, CancellationToken token)
        {
            var map = DynamicQueryMapping.Create(query);
            bool hasCreatedAutoIndex = false;

            Index index;

            while (TryMatchExistingIndexToQuery(map, out index) == false)
            {
                if (createAutoIndexIfNoMatchIsFound == false)
                    throw new IndexDoesNotExistException("Could not find index for a given query.");

                var definition = map.CreateAutoIndexDefinition();
                index = await _indexStore.CreateIndex(definition, RaftIdGenerator.NewId());
                if (index == null)
                {
                    // the index was deleted, we'll try to find a better match (replaced by a wider index)
                    continue;
                }

                hasCreatedAutoIndex = true;

                if (query.WaitForNonStaleResultsTimeout.HasValue == false)
                {
                    query.WaitForNonStaleResultsTimeout = customStalenessWaitTimeout ?? TimeSpan.FromSeconds(15);
                }

                var t = CleanupSupersededAutoIndexes(index, map, RaftIdGenerator.NewId(), token)
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
                    }, token);

                if (query.WaitForNonStaleResults &&
                    Database.Configuration.Indexing.TimeToWaitBeforeDeletingAutoIndexMarkedAsIdle.AsTimeSpan ==
                    TimeSpan.Zero)
                    await t; // this is used in testing, mainly

                break;
            }

            return (index, hasCreatedAutoIndex);
        }

        private async Task CleanupSupersededAutoIndexes(Index index, DynamicQueryMapping map, string raftRequestId, CancellationToken token)
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
                            await _indexStore.DeleteIndex(supersededIndex.Name, $"{raftRequestId}/{supersededIndex.Name}");
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

        public List<DynamicQueryToIndexMatcher.Explanation> ExplainIndexSelection(IndexQueryServerSide query, out string indexName)
        {
            var map = DynamicQueryMapping.Create(query);
            var explanations = new List<DynamicQueryToIndexMatcher.Explanation>();

            var dynamicQueryToIndex = new DynamicQueryToIndexMatcher(_indexStore);
            var match = dynamicQueryToIndex.Match(map, explanations);
            indexName = match.IndexName;

            return explanations;
        }

        private bool TryMatchExistingIndexToQuery(DynamicQueryMapping map, out Index index)
        {
            var dynamicQueryToIndex = new DynamicQueryToIndexMatcher(_indexStore);

            var matchResult = dynamicQueryToIndex.Match(map);

            switch (matchResult.MatchType)
            {
                case DynamicQueryMatchType.Complete:
                case DynamicQueryMatchType.CompleteButIdle:
                    index = GetIndex(matchResult.IndexName, throwIfNotExists: false);
                    if (index == null)
                    {
                        // the auto index was deleted
                        break;
                    }

                    return true;
                case DynamicQueryMatchType.Partial:
                    // At this point, we found an index that has some fields we need and
                    // isn't incompatible with anything else we're asking for
                    // We need to clone that other index
                    // We need to add all our requested indexes information to our cloned index
                    // We can then use our new index instead

                    var currentIndex = _indexStore.GetIndex(matchResult.IndexName);
                    if (currentIndex != null)
                    {
                        if (map.SupersededIndexes == null)
                            map.SupersededIndexes = new List<Index>();

                        map.SupersededIndexes.Add(currentIndex);

                        map.ExtendMappingBasedOn((AutoIndexDefinitionBase)currentIndex.Definition);
                    }

                    break;
            }

            index = null;
            return false;
        }
    }
}
