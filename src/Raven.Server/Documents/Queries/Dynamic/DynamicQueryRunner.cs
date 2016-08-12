using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Abstractions.Util;
using Raven.Server.Documents.Includes;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Queries.Results;
using Raven.Server.Documents.Transformers;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Queries.Dynamic
{
    public class DynamicQueryRunner
    {
        private const string DynamicIndexPrefix = "dynamic/";

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

        public Task<DocumentQueryResult> Execute(string dynamicIndexName, IndexQueryServerSide query, long? existingResultEtag)
        {
            var collection = dynamicIndexName.Substring(DynamicIndexPrefix.Length);

            var map = DynamicQueryMapping.Create(collection, query);

            if (map.MapFields.Length == 0 && map.SortDescriptors.Length == 0 && map.GroupByFields.Length == 0)
            {
                // we optimize for empty queries without sorting options
                var result = new DocumentQueryResult
                {
                    IndexName = collection,
                    IsStale = false,
                    ResultEtag = Environment.TickCount,
                    LastQueryTime = DateTime.MinValue,
                    IndexTimestamp = DateTime.MinValue
                };

                _context.OpenReadTransaction();

                var collectionStats = _documents.GetCollection(collection, _context);

                result.TotalResults = (int)collectionStats.Count;

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
                    var documents = _documents.GetDocumentsAfter(_context, collection, 0, query.Start, query.PageSize);
                    var results = scope != null ? scope.Transform(documents) : documents;

                    foreach (var document in results)
                    {
                        _token.Token.ThrowIfCancellationRequested();

                        var doc = fieldsToFetch.IsProjection
                            ? MapQueryResultRetriever.GetProjectionFromDocument(document, fieldsToFetch, _context)
                            : document;

                        result.Results.Add(doc);
                        includeDocumentsCommand.Gather(doc);
                    }
                }

                includeDocumentsCommand.Fill(result.Includes);

                return new CompletedTask<DocumentQueryResult>(result);
            }

            Index index;
            if (TryMatchExistingIndexToQuery(map, out index) == false)
            {
                var definition = map.CreateAutoIndexDefinition();

                var id = _indexStore.CreateIndex(definition);
                index = _indexStore.GetIndex(id);

                if (query.WaitForNonStaleResultsTimeout.HasValue == false)
                    query.WaitForNonStaleResultsTimeout = TimeSpan.FromSeconds(15); // allow new auto indexes to have some results
            }
            else
            {
                var currentIndexEtag = index.GetIndexEtag();

                if (existingResultEtag == currentIndexEtag)
                {
                    return new CompletedTask<DocumentQueryResult>(new DocumentQueryResult
                    {
                        NotModified = true
                    });
                }
            }

            query = EnsureValidQuery(query, map);

            return index.Query(query, _context, _token);
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

        private static IndexQueryServerSide EnsureValidQuery(IndexQueryServerSide query, DynamicQueryMapping map)
        {
            foreach (var field in map.MapFields)
            {
                query.Query = query.Query.Replace(field.Name, IndexField.ReplaceInvalidCharactersInFieldName(field.Name));
            }

            return query;
        }
    }
}