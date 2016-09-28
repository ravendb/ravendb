using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Client.Data.Indexes;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Indexes.Auto;
using Raven.Server.Documents.Indexes.MapReduce;
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

            public string Index { get; private set; }
            public string Reason { get; private set; }
        }

        public DynamicQueryMatchResult Match(DynamicQueryMapping query, List<Explanation> explanations = null)
        {
            var definitions = _indexStore.GetIndexesForCollection(query.ForCollection)
                .Where(x => query.IsMapReduce ? x.Type.IsMapReduce() : x.Type.IsMap())
                .Select(x => x.Definition)
                .ToList();

            if (definitions.Count == 0)
                return new DynamicQueryMatchResult(string.Empty, DynamicQueryMatchType.Failure);

            var results = definitions.Select(definition => ConsiderUsageOfIndex(query, definition, explanations))
                    .Where(result => result.MatchType != DynamicQueryMatchType.Failure)
                    .GroupBy(x => x.MatchType)
                    .ToDictionary(x => x.Key, x => x.ToArray());

            DynamicQueryMatchResult[] matchResults;
            if (results.TryGetValue(DynamicQueryMatchType.Complete, out matchResults) && matchResults.Length > 0)
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
                if (definition.Collections.Length == 0)
                    explanations?.Add(new Explanation(indexName, "Query is specific for collection, but the index searches across all of them, may result in a different type being returned."));
                else
                    explanations?.Add(new Explanation(indexName, $"Index does not apply to collection '{collection}'"));

                return new DynamicQueryMatchResult(indexName, DynamicQueryMatchType.Failure);
            }
            else
            {
                if (definition.Collections.Length > 1) // we only allow indexes with a single entity name
                {
                    explanations?.Add(new Explanation(indexName, "Index contains more than a single entity name, may result in a different type being returned."));
                    return new DynamicQueryMatchResult(indexName, DynamicQueryMatchType.Failure);
                }
            }

            var index = _indexStore.GetIndex(definition.Name);

            var priority = index.Priority;
            var stats = index.GetStats(calculateCollectionStats: false);

            if (priority.HasFlag(IndexingPriority.Error) || priority.HasFlag(IndexingPriority.Disabled) || stats.IsInvalidIndex)
            {
                explanations?.Add(new Explanation(indexName, $"Cannot do dynamic queries on disabled index or index with errors (index name = {indexName})"));
                return new DynamicQueryMatchResult(indexName, DynamicQueryMatchType.Failure);
            }

            var currentBestState = DynamicQueryMatchType.Complete;

            foreach (var field in query.MapFields)
            {
                IndexField indexField;
                if (definition.TryGetField(index.Type.IsAuto() ? field.Name : field.NormalizedName, out indexField))
                {
                    if (string.IsNullOrWhiteSpace(indexField.Analyzer) == false)
                    {
                        explanations?.Add(new Explanation(indexName, $"The following field have a custom analyzer: {indexField.Name}"));
                        currentBestState = DynamicQueryMatchType.Partial;
                    }

                    if (indexField.Indexing != FieldIndexing.Default)
                    {
                        explanations?.Add(new Explanation(indexName, $"The following field is not using default indexing: {indexField.Name}"));
                        currentBestState = DynamicQueryMatchType.Partial;
                    }
                }
                else
                {
                    explanations?.Add(new Explanation(indexName, $"The following field is missing: {indexField.Name}"));
                    currentBestState = DynamicQueryMatchType.Partial;
                }
            }

            //TODO arek: ignore highlighting for now

            foreach (var sortInfo in query.SortDescriptors) // with matching sort options
            {
                var sortFieldName = index.Type.IsAuto() ? sortInfo.Name : sortInfo.NormalizedName;

                if (sortFieldName.StartsWith(Constants.Indexing.Fields.AlphaNumericFieldName) ||
                    sortFieldName.StartsWith(Constants.Indexing.Fields.RandomFieldName) ||
                    sortFieldName.StartsWith(Constants.Indexing.Fields.CustomSortFieldName))
                {
                    sortFieldName = SortFieldHelper.ExtractName(sortFieldName);
                }

                if (sortFieldName.EndsWith(Constants.Indexing.Fields.RangeFieldSuffix))
                    sortFieldName = sortFieldName.Substring(0, sortFieldName.Length - Constants.Indexing.Fields.RangeFieldSuffix.Length);

                IndexField indexField = null;
                // if the field is not in the output, then we can't sort on it. 
                if (definition.ContainsField(sortFieldName) == false)
                {
                    if (query.IsMapReduce == false)
                    {
                        explanations?.Add(new Explanation(indexName, $"Rejected because index does not contains field '{sortFieldName}' which we need to sort on"));
                        currentBestState = DynamicQueryMatchType.Partial;
                        continue;
                    }

                    // for map-reduce queries try to get field from group by fields as well
                    var autoMapReduceIndexDefinition = definition as AutoMapReduceIndexDefinition;
                    if (autoMapReduceIndexDefinition != null)
                    {
                        if (autoMapReduceIndexDefinition.GroupByFields.TryGetValue(sortFieldName, out indexField) == false)
                        {
                            explanations?.Add(new Explanation(indexName, $"Rejected because index does not contains field '{sortFieldName}' which we need to sort on"));
                            currentBestState = DynamicQueryMatchType.Partial;
                            continue;
                        }
                    }
                    else
                    {
                        var mapReduceIndexDefinition = definition as MapReduceIndexDefinition;
                        if (mapReduceIndexDefinition != null)
                        {
                            throw new NotImplementedException("TODO arek");
                        }
                    }
                }
                else
                {
                    indexField = definition.GetField(sortFieldName);
                }

                Debug.Assert(indexField != null);

                if (sortInfo.FieldType != indexField.SortOption)
                {
                    if (indexField.SortOption == null)
                    {
                        switch (sortInfo.FieldType) // if field is not sorted, we check if we asked for the default sorting
                        {
                            case SortOptions.String:
                            case SortOptions.None:
                                continue;
                        }
                    }

                    explanations?.Add(new Explanation(indexName,
                            $"The specified sort type ({sortInfo.FieldType}) is different than the one specified for field '{sortFieldName}' ({indexField.SortOption})"));
                    return new DynamicQueryMatchResult(indexName, DynamicQueryMatchType.Failure);
                }
            }

            if (currentBestState == DynamicQueryMatchType.Complete && priority.HasFlag(IndexingPriority.Idle))
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

                if (field.MapReduceOperation != mapField.MapReduceOperation)
                {
                    explanations?.Add(new Explanation(indexName, $"The following field {field.Name} has {field.MapReduceOperation} operation defined, while query required {mapField.MapReduceOperation}"));

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