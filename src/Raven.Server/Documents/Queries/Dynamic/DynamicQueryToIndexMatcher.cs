using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Indexes.MapReduce.Auto;
using Raven.Server.Documents.Indexes.MapReduce.Static;
using Raven.Server.Documents.Queries.Sorting;

namespace Raven.Server.Documents.Queries.Dynamic
{
    public class DynamicQueryMatchResult
    {
        public string IndexName { get; set; }
        public DynamicQueryMatchType MatchType { get; set; }

        public DynamicQueryMatchResult(string match, DynamicQueryMatchType matchType)
        {
            IndexName = match;
            MatchType = matchType;
        }

        public long LastMappedEtag { get; set; }

        public long NumberOfMappedFields { get; set; }
    }

    public enum DynamicQueryMatchType
    {
        Complete,
        Partial,
        Failure
    }

    public class DynamicQueryToIndexMatcher
    {
        private readonly IndexStore _indexStore;

        public DynamicQueryToIndexMatcher(IndexStore indexStore)
        {
            _indexStore = indexStore;
        }

        public class Explanation
        {
            public Explanation(string index, string reason)
            {
                Index = index;
                Reason = reason;
            }

            public string Index { get; }
            public string Reason { get; }
        }

        public DynamicQueryMatchResult Match(DynamicQueryMapping query, List<Explanation> explanations = null)
        {
            var definitions = _indexStore.GetIndexesForCollection(query.ForCollection)
                .Where(x => x.Type.IsAuto() && (query.IsMapReduce ? x.Type.IsMapReduce() : x.Type.IsMap()))
                .Select(x => x.Definition)
                .ToList();

            if (definitions.Count == 0)
                return new DynamicQueryMatchResult(string.Empty, DynamicQueryMatchType.Failure);

            var results = definitions.Select(definition => ConsiderUsageOfIndex(query, definition, explanations))
                    .Where(result => result.MatchType != DynamicQueryMatchType.Failure)
                    .GroupBy(x => x.MatchType)
                    .ToDictionary(x => x.Key, x => x.ToArray());

            if (results.TryGetValue(DynamicQueryMatchType.Complete, out DynamicQueryMatchResult[] matchResults) && matchResults.Length > 0)
            {
                var prioritizedResults = matchResults
                    .OrderByDescending(x => x.LastMappedEtag)
                    .ThenByDescending(x => x.NumberOfMappedFields)
                    .ToArray();

                if (explanations != null)
                {
                    for (var i = 1; i < prioritizedResults.Length; i++)
                    {
                        explanations.Add(new Explanation(prioritizedResults[i].IndexName, "Wasn't the widest / most unstable index matching this query"));
                    }
                }

                return prioritizedResults[0];
            }

            if (results.TryGetValue(DynamicQueryMatchType.Partial, out matchResults) && matchResults.Length > 0)
            {
                return matchResults.OrderByDescending(x => x.NumberOfMappedFields).First();
            }

            return new DynamicQueryMatchResult(string.Empty, DynamicQueryMatchType.Failure);
        }

        private DynamicQueryMatchResult ConsiderUsageOfIndex(DynamicQueryMapping query, IndexDefinitionBase definition, List<Explanation> explanations = null)
        {
            var collection = query.ForCollection;
            var indexName = definition.Name;

            if (definition.Collections.Contains(collection, StringComparer.OrdinalIgnoreCase) == false)
            {
                if (definition.Collections.Count == 0)
                    explanations?.Add(new Explanation(indexName, "Query is specific for collection, but the index searches across all of them, may result in a different type being returned."));
                else
                    explanations?.Add(new Explanation(indexName, $"Index does not apply to collection '{collection}'"));

                return new DynamicQueryMatchResult(indexName, DynamicQueryMatchType.Failure);
            }

            if (definition.Collections.Count > 1) // we only allow indexes with a single entity name
            {
                explanations?.Add(new Explanation(indexName, "Index contains more than a single entity name, may result in a different type being returned."));
                return new DynamicQueryMatchResult(indexName, DynamicQueryMatchType.Failure);
            }

            var index = _indexStore.GetIndex(definition.Name);

            var state = index.State;
            var stats = index.GetStats();

            if (state == IndexState.Error || state == IndexState.Disabled|| stats.IsInvalidIndex)
            {
                explanations?.Add(new Explanation(indexName, $"Cannot do dynamic queries on disabled index or index with errors (index name = {indexName})"));
                return new DynamicQueryMatchResult(indexName, DynamicQueryMatchType.Failure);
            }

            var currentBestState = DynamicQueryMatchType.Complete;

            foreach (var field in query.MapFields)
            {
                if (definition.TryGetField(field.Name, out var indexField))
                {
                    if (field.IsFullTextSearch && indexField.Indexing != FieldIndexing.Analyzed)
                    {
                        explanations?.Add(new Explanation(indexName, $"The following field is not analyzed {indexField.Name}, while the query needs to perform full text search on it"));
                        return new DynamicQueryMatchResult(indexName, DynamicQueryMatchType.Failure);
                    }

                    if (field.IsFullTextSearch == false && indexField.Indexing == FieldIndexing.Analyzed)
                    {
                        explanations?.Add(new Explanation(indexName, $"The following is analyzed {indexField.Name}, while the query asks for non analyzed values"));
                        return new DynamicQueryMatchResult(indexName, DynamicQueryMatchType.Failure);
                    }
                }
                else
                {
                    explanations?.Add(new Explanation(indexName, $"The following field is missing: {field.Name}"));
                    currentBestState = DynamicQueryMatchType.Partial;
                }
            }

            if (currentBestState == DynamicQueryMatchType.Complete && state == IndexState.Idle)
            {
                currentBestState = DynamicQueryMatchType.Partial;
                explanations?.Add(new Explanation(indexName, $"The index (name = {indexName}) is disabled or abandoned. The preference is for active indexes - making a partial match"));
            }

            if (currentBestState != DynamicQueryMatchType.Failure && query.IsMapReduce)
            {
                if (AssertMapReduceFields(query, (AutoMapReduceIndexDefinition)definition, currentBestState, explanations) == false)
                {
                    return new DynamicQueryMatchResult(indexName, DynamicQueryMatchType.Failure);
                }
            }

            if (currentBestState == DynamicQueryMatchType.Partial && index.Type.IsStatic()) // we cannot support this because we might extend fields from static index into auto index
                return new DynamicQueryMatchResult(indexName, DynamicQueryMatchType.Failure);

            return new DynamicQueryMatchResult(indexName, currentBestState)
            {
                LastMappedEtag = index.GetLastMappedEtagFor(collection),
                NumberOfMappedFields = definition.MapFields.Count
            };
        }

        private bool AssertMapReduceFields(DynamicQueryMapping query, AutoMapReduceIndexDefinition definition, DynamicQueryMatchType currentBestState, List<Explanation> explanations)
        {
            var indexName = definition.Name;

            foreach (var mapField in query.MapFields)
            {
                if (definition.ContainsField(mapField.Name) == false)
                {
                    Debug.Assert(currentBestState == DynamicQueryMatchType.Partial);
                    continue;
                }

                var field = definition.GetField(mapField.Name);

                if (field.Aggregation != mapField.AggregationOperation)
                {
                    explanations?.Add(new Explanation(indexName, $"The following field {field.Name} has {field.Aggregation} operation defined, while query required {mapField.AggregationOperation}"));

                    return false;
                }
            }

            if (query.GroupByFields.All(definition.ContainsGroupByField) == false)
            {
                if (explanations != null)
                {
                    var missingFields = query.GroupByFields.Where(x => definition.ContainsGroupByField(x) == false);
                    explanations?.Add(new Explanation(indexName, $"The following group by fields are missing: {string.Join(", ", missingFields)}"));
                }

                return false;
            }

            if (query.GroupByFields.Length != definition.GroupByFields.Count)
            {
                if (explanations != null)
                {
                    var extraFields = definition.GroupByFields.Where(x => query.GroupByFields.Contains(x.Key) == false);
                    explanations?.Add(new Explanation(indexName, $"Index {indexName} has additional group by fields: {string.Join(", ", extraFields)}"));
                }

                return false;
            }

            return true;
        }
    }
}