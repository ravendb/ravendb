using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Util;
using Raven.Server.Documents.Includes;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Transformers;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Queries.Dynamic
{
    public class DynamicQueryRunner
    {
        public const string DynamicIndex = "dynamic";

        public const string DynamicIndexPrefix = "dynamic/";

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

        public Task ExecuteStream(HttpResponse response, BlittableJsonTextWriter writer, string dynamicIndexName, IndexQueryServerSide query)
        {
            string collection;
            var index = MatchIndex(dynamicIndexName, query, false, out collection);
            if (index == null)
            {
                using (var result = new StreamDocumentQueryResult(response, writer, _context))
                    ExecuteCollectionQuery(result, query, collection);

                return Task.CompletedTask;
            }

            return index.StreamQuery(response, writer, query, _context, _token);
        }

        public Task<DocumentQueryResult> Execute(string dynamicIndexName, IndexQueryServerSide query, long? existingResultEtag)
        {
            string collection;
            var index = MatchIndex(dynamicIndexName, query, true, out collection);
            if (index == null)
            {
                var result = new DocumentQueryResult();
                ExecuteCollectionQuery(result, query, collection);
                return new CompletedTask<DocumentQueryResult>(result);
            }

            var currentIndexEtag = index.GetIndexEtag();

            if (existingResultEtag == currentIndexEtag)
                return new CompletedTask<DocumentQueryResult>(DocumentQueryResult.NotModifiedResult);

            return index.Query(query, _context, _token);
        }

        private void ExecuteCollectionQuery(QueryResultServerSide resultToFill, IndexQueryServerSide query, string collection)
        {
            // we optimize for empty queries without sorting options
            resultToFill.IndexName = collection;
            resultToFill.IsStale = false;
            resultToFill.ResultEtag = Environment.TickCount;
            resultToFill.LastQueryTime = DateTime.MinValue;
            resultToFill.IndexTimestamp = DateTime.MinValue;

            _context.OpenReadTransaction();

            var collectionStats = _documents.GetCollection(collection, _context);

            resultToFill.TotalResults = (int)collectionStats.Count;

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

        private Index MatchIndex(string dynamicIndexName, IndexQueryServerSide query, bool createAutoIndexIfNoMatchIsFound, out string collection)
        {
            collection = dynamicIndexName.Length == DynamicIndex.Length
                ? Constants.Indexing.AllDocumentsCollection
                : dynamicIndexName.Substring(DynamicIndexPrefix.Length);

            var map = DynamicQueryMapping.Create(collection, query);

            if (map.MapFields.Length == 0 && map.SortDescriptors.Length == 0 && map.GroupByFields.Length == 0)
                return null; // use collection query

            Index index;
            if (TryMatchExistingIndexToQuery(map, out index) == false)
            {
                if (createAutoIndexIfNoMatchIsFound == false)
                    throw new IndexDoesNotExistsException("Could not find index for a given query.");

                var definition = map.CreateAutoIndexDefinition();

                var id = _indexStore.CreateIndex(definition);
                index = _indexStore.GetIndex(id);

                if (query.WaitForNonStaleResultsTimeout.HasValue == false)
                    query.WaitForNonStaleResultsTimeout = TimeSpan.FromSeconds(15); // allow new auto indexes to have some results
            }

            EnsureValidQuery(query, map);

            return index;
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