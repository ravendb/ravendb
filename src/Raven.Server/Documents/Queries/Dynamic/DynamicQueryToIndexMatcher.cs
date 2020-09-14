using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using Raven.Client.Documents.Indexes;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Indexes.Auto;
using Raven.Server.Documents.Indexes.MapReduce.Auto;

namespace Raven.Server.Documents.Queries.Dynamic
{
    public class DynamicQueryMatchResult
    {
        public static DynamicQueryMatchResult Failure = new DynamicQueryMatchResult("Failure/None", DynamicQueryMatchType.Failure)
        {
            LastMappedEtag = -1,
            NumberOfMappedFields = -1
        };

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
        Failure,
        Partial,
        CompleteButIdle,
        Complete,
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
            var bestComplete = DynamicQueryMatchResult.Failure;

            foreach (var index in _indexStore.GetIndexesForCollection(query.ForCollection))
            {
                if (query.IsGroupBy)
                {
                    if (index.Type != IndexType.AutoMapReduce)
                        continue;
                }
                else if (index.Type != IndexType.AutoMap)
                    continue;

                var auto = (AutoIndexDefinitionBase)index.Definition;

                var result = ConsiderUsageOfIndex(query, auto, explanations);

                string reason = null;
                bool hasBetterMatch = false;
                if (result.MatchType != bestComplete.MatchType)
                {
                    hasBetterMatch = result.MatchType > bestComplete.MatchType;
                    reason = "A better match was available";
                }
                else if (result.LastMappedEtag != bestComplete.LastMappedEtag)
                {
                    hasBetterMatch = result.LastMappedEtag > bestComplete.LastMappedEtag;
                    reason = "Wasn't the most up to date index matching this query";
                }
                else if (result.NumberOfMappedFields != bestComplete.NumberOfMappedFields)
                {
                    hasBetterMatch = result.NumberOfMappedFields > bestComplete.NumberOfMappedFields;
                    reason = "Wasn't the widest index matching this query";
                }

                if (explanations != null &&
                    bestComplete.MatchType != DynamicQueryMatchType.Failure &&
                    reason != null)
                {
                    var indexName = hasBetterMatch ? bestComplete.IndexName : result.IndexName;
                    explanations.Add(new Explanation(indexName, reason));
                }

                if (hasBetterMatch)
                    bestComplete = result;
            }

            return bestComplete;
        }

        private static DynamicQueryMatchResult SelectIndexMatchingCompletely(List<Explanation> explanations, DynamicQueryMatchResult[] matchResults)
        {
            var prioritizedResults = matchResults
                .OrderByDescending(x => x.LastMappedEtag)
                .ThenByDescending(x => x.NumberOfMappedFields)
                .ToArray();

            if (explanations != null)
            {
                for (var i = 1; i < prioritizedResults.Length; i++)
                {
                    explanations.Add(new Explanation(prioritizedResults[i].IndexName, "Wasn't the widest / most up to date index matching this query"));
                }
            }

            return prioritizedResults[0];
        }

        internal DynamicQueryMatchResult ConsiderUsageOfIndex(DynamicQueryMapping query, AutoIndexDefinitionBase definition, List<Explanation> explanations = null)
        {
            var collection = query.ForCollection;
            var indexName = definition.Name;

            if (definition.Collections.Contains(collection) == false)
            {
                explanations?.Add(new Explanation(indexName,
                    definition.Collections.Count == 0
                        ? "Query is specific for collection, but the index searches across all of them, may result in a different type being returned."
                        : $"Index does not apply to collection '{collection}'"));

                return new DynamicQueryMatchResult(indexName, DynamicQueryMatchType.Failure);
            }

            if (definition.Collections.Count > 1) // we only allow indexes with a single entity name
            {
                explanations?.Add(new Explanation(indexName, "Index contains more than a single entity name, may result in a different type being returned."));
                return new DynamicQueryMatchResult(indexName, DynamicQueryMatchType.Failure);
            }

            var index = _indexStore.GetIndex(definition.Name);
            if (index == null)
                return new DynamicQueryMatchResult(definition.Name, DynamicQueryMatchType.Failure);

            var state = index.State;
            bool isInvalidStats;
            try
            {
                isInvalidStats = index.IsInvalidIndex();
            }
            catch (OperationCanceledException)
            {
                return new DynamicQueryMatchResult(definition.Name, DynamicQueryMatchType.Failure);
            }
            if (state == IndexState.Error || state == IndexState.Disabled || isInvalidStats)
            {
                explanations?.Add(new Explanation(indexName, $"Cannot do dynamic queries on disabled index or index with errors (index name = {indexName})"));
                return new DynamicQueryMatchResult(indexName, DynamicQueryMatchType.Failure);
            }

            var currentBestState = DynamicQueryMatchType.Complete;

            foreach (var field in query.MapFields.Values)
            {
                if (definition.TryGetField(field.Name, out var indexField))
                {
                    if (field.IsFullTextSearch && indexField.Indexing.HasFlag(AutoFieldIndexing.Search) == false)
                    {
                        explanations?.Add(new Explanation(indexName, $"The following field is not searchable {indexField.Name}, while the query needs to search() on it"));
                        return new DynamicQueryMatchResult(indexName, DynamicQueryMatchType.Partial);
                    }

                    if (field.HasHighlighting && indexField.Indexing.HasFlag(AutoFieldIndexing.Highlighting) == false)
                    {
                        explanations?.Add(new Explanation(indexName, $"The following field does not have highlighting {indexField.Name}, while the query needs to do highlight() on it"));
                        return new DynamicQueryMatchResult(indexName, DynamicQueryMatchType.Partial);
                    }

                    if (field.IsExactSearch && indexField.Indexing.HasFlag(AutoFieldIndexing.Exact) == false)
                    {
                        explanations?.Add(new Explanation(indexName, $"The following field is not exactable {indexField.Name}, while the query needs to perform exact() on it"));
                        return new DynamicQueryMatchResult(indexName, DynamicQueryMatchType.Partial);
                    }

                    if (field.Spatial != null)
                    {
                        if (field.Spatial.Equals(indexField.Spatial) == false)
                        {
                            explanations?.Add(new Explanation(indexName, $"The following field is not a spatial field {indexField.Name}, while the query needs to perform spatial() on it"));
                            return new DynamicQueryMatchResult(indexName, DynamicQueryMatchType.Failure);
                        }
                    }

                    if (field.HasSuggestions && indexField.HasSuggestions == false)
                    {
                        explanations?.Add(new Explanation(indexName, $"The following field does not have suggestions enabled {indexField.Name}, while the query needs to perform suggest() on it"));
                        return new DynamicQueryMatchResult(indexName, DynamicQueryMatchType.Partial);
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
                currentBestState = DynamicQueryMatchType.CompleteButIdle;
                explanations?.Add(new Explanation(indexName, $"The index (name = {indexName}) is idle. The preference is for active indexes - making a complete match but marking the index is idle"));
            }

            if (currentBestState != DynamicQueryMatchType.Failure && query.IsGroupBy)
            {
                var bestMapReduceMatch = AssertMapReduceFields(query, (AutoMapReduceIndexDefinition)definition, currentBestState, explanations);

                if (bestMapReduceMatch != DynamicQueryMatchType.Complete)
                    return new DynamicQueryMatchResult(indexName, bestMapReduceMatch);
            }

            long lastMappedEtagFor;
            try
            {
                lastMappedEtagFor = index.GetLastMappedEtagFor(collection);
            }
            catch (OperationCanceledException)
            {
                // the index was disposed while we were reading it, just ignore it
                // probably dynamic index that was disposed by the auto cleaner
                return new DynamicQueryMatchResult(indexName, DynamicQueryMatchType.Failure);
            }

            return new DynamicQueryMatchResult(indexName, currentBestState)
            {
                LastMappedEtag = lastMappedEtagFor,
                NumberOfMappedFields = definition.MapFields.Count
            };
        }

        private static DynamicQueryMatchType AssertMapReduceFields(DynamicQueryMapping query, AutoMapReduceIndexDefinition definition, DynamicQueryMatchType currentBestState,
            List<Explanation> explanations)
        {
            var indexName = definition.Name;

            foreach (var mapField in query.MapFields.Values)
            {
                if (definition.ContainsField(mapField.Name) == false)
                {
                    Debug.Assert(currentBestState == DynamicQueryMatchType.Partial);
                    continue;
                }

                var field = definition.GetField(mapField.Name);

                if (field.Aggregation != mapField.AggregationOperation)
                {
                    explanations?.Add(new Explanation(indexName,
                        $"The following field {field.Name} has {field.Aggregation} operation defined, while query required {mapField.AggregationOperation}"));

                    return DynamicQueryMatchType.Failure;
                }
            }

            foreach (var groupByField in query.GroupByFields.Values)
            {
                if (definition.GroupByFields.TryGetValue(groupByField.Name, out var indexField))
                {
                    if (groupByField.GroupByArrayBehavior != indexField.GroupByArrayBehavior)
                    {
                        explanations?.Add(new Explanation(indexName,
                            $"The following group by field {indexField.Name} is grouping by '{indexField.GroupByArrayBehavior}', while the query needs to perform '{groupByField.GroupByArrayBehavior}' grouping"));

                        return DynamicQueryMatchType.Failure;
                    }

                    if (groupByField.IsSpecifiedInWhere == false)
                        continue;

                    if (groupByField.IsFullTextSearch && indexField.Indexing.HasFlag(AutoFieldIndexing.Search) == false)
                    {
                        explanations?.Add(new Explanation(indexName,
                            $"The following group by field is not searchable {indexField.Name}, while the query needs to perform search() on it"));

                        return DynamicQueryMatchType.Partial;
                    }

                    if (groupByField.IsExactSearch && indexField.Indexing.HasFlag(AutoFieldIndexing.Exact) == false)
                    {
                        explanations?.Add(new Explanation(indexName,
                            $"The following group by field is not exactable {indexField.Name}, while the query needs to perform exact() on it"));

                        return DynamicQueryMatchType.Partial;
                    }
                }
                else
                {
                    if (explanations != null)
                    {
                        var missingFields = query.GroupByFields.Where(x => definition.GroupByFields.ContainsKey(x.Value.Name) == false);
                        explanations.Add(new Explanation(indexName, $"The following group by fields are missing: {string.Join(", ", missingFields)}"));
                    }

                    return DynamicQueryMatchType.Failure;
                }
            }


            if (query.GroupByFields.Count != definition.GroupByFields.Count)
            {
                if (explanations != null)
                {
                    var extraFields = definition.GroupByFields.Where(x => query.GroupByFields.Select(y => y.Value.Name.Value).Contains(x.Key) == false);
                    explanations.Add(new Explanation(indexName, $"Index {indexName} has additional group by fields: {string.Join(", ", extraFields)}"));
                }

                return DynamicQueryMatchType.Failure;
            }

            return DynamicQueryMatchType.Complete;
        }
    }
}
