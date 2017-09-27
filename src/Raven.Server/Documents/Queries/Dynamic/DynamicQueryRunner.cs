using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Raven.Client.Exceptions.Documents.Indexes;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Indexes.Auto;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Json;
using Sparrow.Utils;

namespace Raven.Server.Documents.Queries.Dynamic
{
    public class DynamicQueryRunner : AbstractQueryRunner
    {
        private readonly IndexStore _indexStore;

        public DynamicQueryRunner(DocumentDatabase database) : base(database)
        {
            _indexStore = database.IndexStore;
        }

        public override async Task ExecuteStreamQuery(IndexQueryServerSide query, DocumentsOperationContext documentsContext, HttpResponse response, BlittableJsonTextWriter writer,
            OperationCancelToken token)
        {
            var index = await MatchIndex(query, false, token.Token);

            if (index == null)
                IndexDoesNotExistException.ThrowFor("There was no auto index able to handle streaming query"); // TODO arek - pretty sure we want to change that behavior

            await index.StreamQuery(response, writer, query, documentsContext, token);
        }

        public override async Task<DocumentQueryResult> ExecuteQuery(IndexQueryServerSide query, DocumentsOperationContext documentsContext, long? existingResultEtag, OperationCancelToken token)
        {
            var index = await MatchIndex(query, true, token.Token);

            if (existingResultEtag.HasValue)
            {
                var etag = index.GetIndexEtag();
                if (etag == existingResultEtag)
                    return DocumentQueryResult.NotModifiedResult;
            }

            return await index.Query(query, documentsContext, token);
        }

        public override async Task<IndexEntriesQueryResult> ExecuteIndexEntriesQuery(IndexQueryServerSide query, DocumentsOperationContext context, long? existingResultEtag, OperationCancelToken token)
        {
            var index = await MatchIndex(query, false, token.Token);

            if (index == null)
                IndexDoesNotExistException.ThrowFor(query.Metadata.CollectionName);

            if (existingResultEtag.HasValue)
            {
                var etag = index.GetIndexEtag();
                if (etag == existingResultEtag)
                    return IndexEntriesQueryResult.NotModifiedResult;
            }

            return index.IndexEntries(query, context, token);
        }

        private async Task<Index> MatchIndex(IndexQueryServerSide query, bool createAutoIndexIfNoMatchIsFound, CancellationToken token)
        {
            if (query.Metadata.DynamicIndexName != null)
                return _indexStore.GetIndex(query.Metadata.DynamicIndexName);

            var map = DynamicQueryMapping.Create(query);

            if (TryMatchExistingIndexToQuery(map, out Index index) == false)
            {
                if (createAutoIndexIfNoMatchIsFound == false)
                    throw new IndexDoesNotExistException("Could not find index for a given query.");

                var definition = map.CreateAutoIndexDefinition();

                var id = await _indexStore.CreateIndex(definition);
                index = _indexStore.GetIndex(id);

                var t = CleanupSupercededAutoIndexes(index, map, token)
                    .ContinueWith(task =>
                    {
                        if (task.Exception != null)
                        {
                            if (token.IsCancellationRequested)
                                return;

                            if (_indexStore.Logger.IsInfoEnabled)
                            {
                                _indexStore.Logger.Info("Failed to delete superceded indexes for index " + index.Name);
                            }
                        }
                    });

                if (query.WaitForNonStaleResults && 
                    Database.Configuration.Indexing.TimeToWaitBeforeDeletingAutoIndexMarkedAsIdle.AsTimeSpan ==
                    TimeSpan.Zero)
                    await t; // this is used in testing, mainly

                if (query.WaitForNonStaleResultsTimeout.HasValue == false)
                    query.WaitForNonStaleResultsTimeout = TimeSpan.FromSeconds(15); // allow new auto indexes to have some results
            }

            return index;
        }

        private async Task CleanupSupercededAutoIndexes(Index index, DynamicQueryMapping map, CancellationToken token)
        {
            if (map.SupersededIndexes == null || map.SupersededIndexes.Count == 0)
                return;

            // this is meant to remove superceded indexes immediately when they are of no use
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

                var maxSupercededEtag = 0L;
                foreach (var supercededIndex in map.SupersededIndexes)
                {
                    var etag = supercededIndex.GetLastMappedEtagFor(map.ForCollection);
                    maxSupercededEtag = Math.Max(etag, maxSupercededEtag);
                }

                var currentEtag = index.GetLastMappedEtagFor(map.ForCollection);
                if (currentEtag >= maxSupercededEtag)
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

                    foreach (var supercededIndex in map.SupersededIndexes)
                    {
                        try
                        {
                            await _indexStore.DeleteIndex(supercededIndex.Etag);
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

        public List<DynamicQueryToIndexMatcher.Explanation> ExplainIndexSelection(IndexQueryServerSide query)
        {
            var map = DynamicQueryMapping.Create(query);
            var explanations = new List<DynamicQueryToIndexMatcher.Explanation>();

            var dynamicQueryToIndex = new DynamicQueryToIndexMatcher(_indexStore);
            dynamicQueryToIndex.Match(map, explanations);

            return explanations;
        }

        private bool TryMatchExistingIndexToQuery(DynamicQueryMapping map, out Index index)
        {
            var dynamicQueryToIndex = new DynamicQueryToIndexMatcher(_indexStore);

            var matchResult = dynamicQueryToIndex.Match(map);

            switch (matchResult.MatchType)
            {
                case DynamicQueryMatchType.Complete:
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
