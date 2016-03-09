using System;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using Microsoft.AspNet.Server.Kestrel.Networking;
using Raven.Abstractions.Data;
using Raven.Server.Documents.Indexes;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Queries.Dynamic
{
    public class DynamicQueryRunner
    {
        private const string DynamicIndexPrefix = "dynamic/";

        private readonly IndexStore _indexStore;
        private readonly DocumentsOperationContext _context;

        public DynamicQueryRunner(IndexStore indexStore, DocumentsOperationContext context)
        {
            _indexStore = indexStore;
            _context = context;
        }

        public DocumentQueryResult Execute(string dynamicIndexName, IndexQuery query)
        {
            var collection = dynamicIndexName.Substring(DynamicIndexPrefix.Length);

            var map = DynamicQueryMapping.Create(collection, query);

            bool newAutoIndex = false;

            Index index;
            if (TryMatchExistingIndexToQuery(map, out index) == false)
            {
                var autoIndexDef = map.CreateAutoIndexDefinition();

                var id = _indexStore.CreateIndex(autoIndexDef);
                index = _indexStore.GetIndex(id);

                newAutoIndex = true;
            }

            //TODO arek
            //string realQuery = map.Items.Aggregate(query.Query, (current, mapItem) => current.Replace(mapItem.QueryFrom, mapItem.To));

            //UpdateFieldNamesForSortedFields(query, map);

            // We explicitly do NOT want to update the field names of FieldsToFetch - that reads directly from the document
            //UpdateFieldsInArray(map, query.FieldsToFetch);

            return ExecuteActualQuery(index, query, newAutoIndex);
        }

        private static void UpdateFieldNamesForSortedFields(IndexQuery query, DynamicQueryMapping map)
        {
            if (query.SortedFields == null) return;
            foreach (var sortedField in query.SortedFields)
            {
                var item = map.MapFields.FirstOrDefault(x => x.From == sortedField.Field);
                if (item != null)
                    sortedField.Field = item.To;
            }
        }

        private DocumentQueryResult ExecuteActualQuery(Index index, IndexQuery query, bool newAutoIndex)
        {
            // Perform the query until we have some results at least
            var sp = Stopwatch.StartNew();

            while (true)
            {
                var result = index.Query(query, _context, CancellationToken.None); // TODO arek

                if (newAutoIndex == false ||
                    result.IsStale == false ||
                    (result.Results.Count >= query.PageSize && query.PageSize > 0) ||
                    sp.Elapsed.TotalSeconds > 15)
                {
                    return result;
                }

                _context.Reset(); // dispose already open read transactions - TODO arek
                Thread.Sleep(100);
            }
        }

        private static IndexQuery CreateIndexQuery(IndexQuery query, DynamicQueryMapping map, string realQuery)
        {
            var indexQuery = new IndexQuery
            {
                Cutoff = query.Cutoff,
                WaitForNonStaleResultsAsOfNow = query.WaitForNonStaleResultsAsOfNow,
                PageSize = query.PageSize,
                Query = realQuery,
                Start = query.Start,
                FieldsToFetch = query.FieldsToFetch,
                IsDistinct = query.IsDistinct,
                SortedFields = query.SortedFields,
                DefaultField = query.DefaultField,
                CutoffEtag = query.CutoffEtag,
                DebugOptionGetIndexEntries = query.DebugOptionGetIndexEntries,
                DefaultOperator = query.DefaultOperator,
                SkippedResults = query.SkippedResults,
                HighlighterPreTags = query.HighlighterPreTags,
                HighlighterPostTags = query.HighlighterPostTags,
                HighlightedFields = query.HighlightedFields,
                HighlighterKeyName = query.HighlighterKeyName,
                ResultsTransformer = query.ResultsTransformer,
                TransformerParameters = query.TransformerParameters,
                ExplainScores = query.ExplainScores,
            };
            if (indexQuery.SortedFields == null)
                return indexQuery;

            for (int index = 0; index < indexQuery.SortedFields.Length; index++)
            {
                var sortedField = indexQuery.SortedFields[index];
                var fieldName = sortedField.Field;
                bool hasRange = false;
                if (fieldName.EndsWith("_Range"))
                {
                    fieldName = fieldName.Substring(0, fieldName.Length - "_Range".Length);
                    hasRange = true;
                }

                var item = map.MapFields.FirstOrDefault(x => string.Equals(x.QueryFrom, fieldName, StringComparison.OrdinalIgnoreCase));
                if (item == null)
                    continue;

                indexQuery.SortedFields[index] = new SortedField(hasRange ? item.To + "_Range" : item.To);
                indexQuery.SortedFields[index].Descending = sortedField.Descending;
            }
            return indexQuery;
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
                    
                    throw new NotSupportedException("TODO arek");
                    //var currentIndex = documentDatabase.IndexDefinitionStorage.GetIndexDefinition(matchResult.IndexName);
                    //map.AddExistingIndexDefinition(currentIndex, documentDatabase, query);

                    break;
            }

            index = null;
            return false;
        }
    }
}