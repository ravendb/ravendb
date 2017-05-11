using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Raven.Client;
using Raven.Client.Documents.Exceptions.Indexes;
using Raven.Server.Documents.Includes;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Transformers;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Json;
using Sparrow.Utils;

namespace Raven.Server.Documents.Queries.Dynamic
{
    public class DynamicQueryRunner
    {
        public const string DynamicIndex = "dynamic";

        public const string DynamicIndexPrefix = "dynamic/";

        public const string CollectionIndexPrefix = "collection/";

        private readonly IndexStore _indexStore;
        private readonly TransformerStore _transformerStore;
        private readonly DocumentsOperationContext _context;
        private readonly DocumentsStorage _documents;
        private readonly OperationCancelToken _token;

        public DynamicQueryRunner(IndexStore indexStore, TransformerStore transformerStore, DocumentsStorage documents, DocumentsOperationContext context, OperationCancelToken token)
        {
            _indexStore = indexStore;
            _transformerStore = transformerStore;
            _context = context;
            _token = token;
            _documents = documents;
        }

        public static bool IsDynamicIndex(string indexName)
        {
            if (indexName == null || indexName.Length < DynamicIndex.Length)
                return false;

            if (indexName.StartsWith(DynamicIndex, StringComparison.OrdinalIgnoreCase) == false)
                return false;

            if (indexName.Length == DynamicIndex.Length)
                return true;

            return indexName[DynamicIndex.Length] == '/';
        }

        public async Task ExecuteStream(HttpResponse response, BlittableJsonTextWriter writer, string dynamicIndexName, IndexQueryServerSide query)
        {
            var tuple = await MatchIndex(dynamicIndexName, query, false);
            var index = tuple.Index;
            var collection = tuple.Collection;
            if (index == null)
            {
                using (var result = new StreamDocumentQueryResult(response, writer, _context))
                {
                    _context.OpenReadTransaction();

                    FillCountOfResultsAndIndexEtag(result, collection);

                    ExecuteCollectionQuery(result, query, collection);
                }

                return;
            }

            await index.StreamQuery(response, writer, query, _context, _token);
        }

        public async Task<DocumentQueryResult> Execute(string dynamicIndexName, IndexQueryServerSide query, long? existingResultEtag)
        {
            var tuple = await MatchIndex(dynamicIndexName, query, true);
            var index = tuple.Index;
            var collection = tuple.Collection;
            if (index == null)
            {
                var result = new DocumentQueryResult();
                _context.OpenReadTransaction();
                FillCountOfResultsAndIndexEtag(result, collection);

                if (existingResultEtag.HasValue)
                {
                    if (result.ResultEtag == existingResultEtag)
                        return DocumentQueryResult.NotModifiedResult;
                }

                ExecuteCollectionQuery(result, query, collection);

                return result;
            }

            if (existingResultEtag.HasValue)
            {
                var etag = index.GetIndexEtag();
                if (etag == existingResultEtag)
                    return DocumentQueryResult.NotModifiedResult;
            }

            return await index.Query(query, _context, _token);
        }

        public async Task<IndexEntriesQueryResult> ExecuteIndexEntries(string dynamicIndexName, IndexQueryServerSide query, long? existingResultEtag)
        {
            var tuple = await MatchIndex(dynamicIndexName, query, false);
            var index = tuple.Index;

            if (index == null)
                IndexDoesNotExistException.ThrowFor(dynamicIndexName);

            if (existingResultEtag.HasValue)
            {
                var etag = index.GetIndexEtag();
                if (etag == existingResultEtag)
                    return IndexEntriesQueryResult.NotModifiedResult;
            }

            return index.IndexEntries(query, _context, _token);
        }

        private void ExecuteCollectionQuery(QueryResultServerSide resultToFill, IndexQueryServerSide query, string collection)
        {
            var isAllDocsCollection = collection == Constants.Documents.Collections.AllDocumentsCollection;

            // we optimize for empty queries without sorting options, appending CollectionIndexPrefix to be able to distinguish index for collection vs. physical index
            resultToFill.IndexName = isAllDocsCollection ? "AllDocs" : CollectionIndexPrefix + collection;
            resultToFill.IsStale = false;
            resultToFill.LastQueryTime = DateTime.MinValue;
            resultToFill.IndexTimestamp = DateTime.MinValue;

            var includeDocumentsCommand = new IncludeDocumentsCommand(_documents, _context, query.Includes);

            Transformer transformer = null;
            if (string.IsNullOrEmpty(query.Transformer) == false)
            {
                transformer = _transformerStore.GetTransformer(query.Transformer);
                if (transformer == null)
                    throw new InvalidOperationException($"The transformer '{query.Transformer}' was not found.");
            }

            using (var scope = transformer?.OpenTransformationScope(query.TransformerParameters, includeDocumentsCommand, _documents, _transformerStore, _context))
            {
                var fieldsToFetch = new FieldsToFetch(query, null, transformer);
                var documents = new CollectionQueryEnumerable(_documents, fieldsToFetch, collection, query, _context);

                var results = scope != null ? scope.Transform(documents) : documents;

                try
                {
                    foreach (var document in results)
                    {
                        _token.Token.ThrowIfCancellationRequested();

                        resultToFill.AddResult(document);

                        includeDocumentsCommand.Gather(document);
                    }
                }
                catch (Exception e)
                {
                    if (resultToFill.SupportsExceptionHandling == false)
                        throw;

                    resultToFill.HandleException(e);
                }
            }

            includeDocumentsCommand.Fill(resultToFill.Includes);
        }

        private unsafe void FillCountOfResultsAndIndexEtag(QueryResultServerSide resultToFill, string collection)
        {
            var buffer = stackalloc long[3];

            if (collection == Constants.Documents.Collections.AllDocumentsCollection)
            {
                var numberOfDocuments = _documents.GetNumberOfDocuments(_context);
                buffer[0] = DocumentsStorage.ReadLastDocumentEtag(_context.Transaction.InnerTransaction);
                buffer[1] = DocumentsStorage.ReadLastTombstoneEtag(_context.Transaction.InnerTransaction);
                buffer[2] = numberOfDocuments;
                resultToFill.TotalResults = (int) numberOfDocuments;
            }
            else
            {
                var collectionStats = _documents.GetCollection(collection, _context);
                resultToFill.TotalResults = (int) collectionStats.Count;

                buffer[0] = _documents.GetLastDocumentEtag(_context, collection);
                buffer[1] = _documents.GetLastTombstoneEtag(_context, collection);
                buffer[2] = collectionStats.Count;
            }

            resultToFill.ResultEtag = (long) Hashing.XXHash64.Calculate((byte*) buffer, sizeof(long) * 3);
        }

        private async Task<(Index Index, string Collection)> MatchIndex(string dynamicIndexName, IndexQueryServerSide query, bool createAutoIndexIfNoMatchIsFound)
        {
            var collection = dynamicIndexName.Length == DynamicIndex.Length
                ? Constants.Documents.Collections.AllDocumentsCollection
                : dynamicIndexName.Substring(DynamicIndexPrefix.Length);

            var map = DynamicQueryMapping.Create(collection, query);

            if (map.MapFields.Length == 0 && map.SortDescriptors.Length == 0 && map.GroupByFields.Length == 0)
                return (null, collection); // use collection query

            Index index;
            if (TryMatchExistingIndexToQuery(map, out index) == false)
            {
                if (createAutoIndexIfNoMatchIsFound == false)
                    throw new IndexDoesNotExistException("Could not find index for a given query.");

                var definition = map.CreateAutoIndexDefinition();

                var id = await _indexStore.CreateIndex(definition);
                index = _indexStore.GetIndex(id);

                var t = CleanupSupercededAutoIndexes(index, map)
                    .ContinueWith(task =>
                    {
                        if (task.Exception != null)
                        {
                            if (_token.Token.IsCancellationRequested)
                                return;

                            if (_indexStore.Logger.IsInfoEnabled)
                            {
                                _indexStore.Logger.Info($"Failed to delete superceded indexes for index " + index.Name);
                            }
                        }
                    });
                GC.KeepAlive(t);// we expicitly don't care about this instance, this line is here to make the compiler happy

                if (query.WaitForNonStaleResultsTimeout.HasValue == false)
                    query.WaitForNonStaleResultsTimeout = TimeSpan.FromSeconds(15); // allow new auto indexes to have some results
            }

            EnsureValidQuery(query, map);

            return (index, collection);
        }

        private async Task CleanupSupercededAutoIndexes(Index index, DynamicQueryMapping map)
        {
            if (map.SupercededIndexes == null || map.SupercededIndexes.Count == 0)
                return;

            // this is meant to remove superceded indexes immediately when they are of no use
            // however, they'll also be cleaned by the idle timer, so we don't worry too much
            // about this being in memory only operation

            while (_token.Token.IsCancellationRequested == false)
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
                foreach (var supercededIndex in map.SupercededIndexes)
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

                    await TimeoutManager.WaitFor(15000).ConfigureAwait(false);

                    foreach (var supercededIndex in map.SupercededIndexes)
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

        public List<DynamicQueryToIndexMatcher.Explanation> ExplainIndexSelection(string dynamicIndexName, IndexQueryServerSide query)
        {
            var collection = dynamicIndexName.Substring(DynamicIndexPrefix.Length);
            var map = DynamicQueryMapping.Create(collection, query);
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

                    if (map.SupercededIndexes == null)
                        map.SupercededIndexes = new List<Index>();

                    map.SupercededIndexes.Add(currentIndex);

                    map.ExtendMappingBasedOn(currentIndex.Definition);

                    break;
            }

            index = null;
            return false;
        }

        private static void EnsureValidQuery(IndexQueryServerSide query, DynamicQueryMapping map)
        {
            foreach (var field in map.MapFields)
            {
                query.Query = query.Query.Replace(field.Name, IndexField.ReplaceInvalidCharactersInFieldName(field.Name));
            }
        }
    }
}