﻿using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Abstractions.Util;
using Raven.Client.Data;
using Raven.Server.Documents.Includes;
using Raven.Server.Documents.Indexes;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Queries.Dynamic
{
    public class DynamicQueryRunner
    {
        private const string DynamicIndexPrefix = "dynamic/";

        private readonly IndexStore _indexStore;
        private readonly DocumentsOperationContext _context;
        private readonly DocumentsStorage _documents;
        private readonly OperationCancelToken _token;

        public DynamicQueryRunner(IndexStore indexStore, DocumentsStorage documents, DocumentsOperationContext context, OperationCancelToken token)
        {
            _indexStore = indexStore;
            _context = context;
            _token = token;
            _documents = documents;
        }

        public Task<DocumentQueryResult> Execute(string dynamicIndexName, IndexQuery query, long? existingResultEtag)
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
                foreach (var document in _documents.GetDocumentsAfter(_context, collection, 0, query.Start, query.PageSize))
                {
                    _token.Token.ThrowIfCancellationRequested();

                    result.Results.Add(document);
                    includeDocumentsCommand.Gather(document);
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

        public List<DynamicQueryToIndexMatcher.Explanation> ExplainIndexSelection(string dynamicIndexName, IndexQuery query)
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

        private static IndexQuery EnsureValidQuery(IndexQuery query, DynamicQueryMapping map)
        {
            foreach (var field in map.MapFields)
            {
                query.Query = query.Query.Replace(field.Name, IndexField.ReplaceInvalidCharactersInFieldName(field.Name));
            }

            return query;
        }
    }
}