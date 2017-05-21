//-----------------------------------------------------------------------
// <copyright file="DynamicQueryRunner.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Concurrent;
using System.Linq;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Database.Data;
using System.Diagnostics;
using System.Threading;

namespace Raven.Database.Queries
{
    public class DynamicQueryRunner
    {
        private readonly DocumentDatabase documentDatabase;
        private readonly object createIndexLock = new object();

        public DynamicQueryRunner(DocumentDatabase database)
        {
            documentDatabase = database;
        }

        public QueryResultWithIncludes ExecuteDynamicQuery(string entityName, IndexQuery query, CancellationToken token)
        {
            // Create the map
            var map = DynamicQueryMapping.Create(documentDatabase, query, entityName);
            var touchTemporaryIndexResult = GetAppropriateIndexToQuery(entityName, query, map);

            string realQuery = map.Items.Aggregate(query.Query,
                                                   (current, mapItem) => current.Replace(mapItem.QueryFrom, mapItem.To));

            UpdateFieldNamesForSortedFields(query, map);

            // We explicitly do NOT want to update the field names of FieldsToFetch - that reads directly from the document
            //UpdateFieldsInArray(map, query.FieldsToFetch);
            
            return ExecuteActualQuery(query, map, touchTemporaryIndexResult, realQuery, token);
        }

        private static void UpdateFieldNamesForSortedFields(IndexQuery query, DynamicQueryMapping map)
        {
            if (query.SortedFields == null) return;
            foreach (var sortedField in query.SortedFields)
            {
                var item = map.Items.FirstOrDefault(x => x.From == sortedField.Field);
                if (item != null)
                    sortedField.Field = item.To;
            }
        }

        private QueryResultWithIncludes ExecuteActualQuery(IndexQuery query, DynamicQueryMapping map, Tuple<string, bool> touchTemporaryIndexResult, string realQuery, CancellationToken token)
        {
            // Perform the query until we have some results at least
            QueryResultWithIncludes result;
            var sp = Stopwatch.StartNew();
            while (true)
            {
                var indexQuery = CreateIndexQuery(query, map, realQuery);
                result = documentDatabase.Queries.Query(map.IndexName, indexQuery, token);

                if (!touchTemporaryIndexResult.Item2 ||
                    !result.IsStale ||
                    (result.Results.Count >= query.PageSize && query.PageSize > 0) ||
                    sp.Elapsed.TotalSeconds > 15)
                {
                    return result;
                }

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
                ShowTimings = query.ShowTimings,
                SortHints = query.SortHints
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
            
                var item = map.Items.FirstOrDefault(x => string.Equals(x.QueryFrom, fieldName, StringComparison.InvariantCultureIgnoreCase));
                if (item == null)
                    continue;

                indexQuery.SortedFields[index] = new SortedField(hasRange ? item.To + "_Range" : item.To);
                indexQuery.SortedFields[index].Descending = sortedField.Descending;
            }
            return indexQuery;
        }

        private Tuple<string, bool> GetAppropriateIndexToQuery(string entityName, IndexQuery query, DynamicQueryMapping map)
        {
            var appropriateIndex = new DynamicQueryOptimizer(documentDatabase).SelectAppropriateIndex(entityName, query);
            if (appropriateIndex.MatchType == DynamicQueryMatchType.Complete)
            {
                map.IndexName = appropriateIndex.IndexName;
                return Tuple.Create(appropriateIndex.IndexName, false);
            }
            
            if (appropriateIndex.MatchType == DynamicQueryMatchType.Partial)
            {
                // At this point, we found an index that has some fields we need and
                // isn't incompatible with anything else we're asking for
                // We need to clone that other index 
                // We need to add all our requested indexes information to our cloned index
                // We can then use our new index instead
                var currentIndex = documentDatabase.IndexDefinitionStorage.GetIndexDefinition(appropriateIndex.IndexName);			
                map.AddExistingIndexDefinition(currentIndex, documentDatabase, query);
            }
            return CreateAutoIndex(map.IndexName, map.CreateIndexDefinition);
        }


        private Tuple<string, bool> CreateAutoIndex(string permanentIndexName, Func<IndexDefinition> createDefinition)
        {
            if (documentDatabase.Indexes.GetIndexDefinition(permanentIndexName) != null)
                return Tuple.Create(permanentIndexName, false);

            lock (createIndexLock)
            {
                var indexDefinition = createDefinition();
                documentDatabase.Indexes.PutIndex(permanentIndexName, indexDefinition);
            }
            
            return Tuple.Create(permanentIndexName, true);
        
        }
    }
}
