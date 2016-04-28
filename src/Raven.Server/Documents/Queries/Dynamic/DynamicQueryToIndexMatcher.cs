using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Client.Data.Indexes;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Indexes.MapReduce;
using Raven.Server.Documents.Queries.Sort;

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

        private delegate void ExplainDelegate(string index, Func<string> rejectionReasonGenerator);

        public class Explanation
        {
            public string Index { get; set; }
            public string Reason { get; set; }
        }

        public DynamicQueryMatchResult Match(DynamicQueryMapping query, List<Explanation> explanations = null)
        {
            ExplainDelegate explain = (index, rejectionReason) => { };
            if (explanations != null)
            {
                explain = (index, rejectionReason) => explanations.Add(new Explanation
                {
                    Index = index,
                    Reason = rejectionReason()
                });
            }

            var definitions = _indexStore.GetIndexDefinitionsForCollection(query.ForCollection,
                query.IsMapReduce ? IndexType.AutoMapReduce : IndexType.AutoMap); // let us work with auto indexes only for now

            if (definitions.Count == 0)
                return new DynamicQueryMatchResult(string.Empty, DynamicQueryMatchType.Failure);

            var results = definitions.Select(definition => ConsiderUsageOfAutoIndex(query, definition, explain))
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

                for (var i = 1; i < prioritizedResults.Length; i++)
                {
                    explain(prioritizedResults[i].IndexName,
                            () => "Wasn't the widest / most unstable index matching this query");
                }

                return prioritizedResults[0];
            }

            if (results.TryGetValue(DynamicQueryMatchType.Partial, out matchResults) && matchResults.Length > 0)
            {
                return matchResults.OrderByDescending(x => x.NumberOfMappedFields).First();
            }

            return new DynamicQueryMatchResult("", DynamicQueryMatchType.Failure);
        }

        private DynamicQueryMatchResult ConsiderUsageOfAutoIndex(DynamicQueryMapping query, IndexDefinitionBase definition, ExplainDelegate explain)
        {
            var collection = query.ForCollection;
            var indexName = definition.Name;

            if (definition.Collections.Contains(collection, StringComparer.OrdinalIgnoreCase) == false)
            {
                if (definition.Collections.Length == 0)
                    explain(indexName, () => "Query is specific for collection, but the index searches across all of them, may result in a different type being returned.");
                else
                    explain(indexName, () => $"Index does not apply to collection '{collection}'");

                return new DynamicQueryMatchResult(indexName, DynamicQueryMatchType.Failure);
            }
            else
            {
                if (definition.Collections.Length > 1) // we only allow indexes with a single entity name
                {
                    explain(indexName, () => "Index contains more than a single entity name, may result in a different type being returned.");
                    return new DynamicQueryMatchResult(indexName, DynamicQueryMatchType.Failure);
                }
            }

            var index = _indexStore.GetIndex(definition.Name);

            var priority = index.Priority;
            var stats = index.GetStats();

            if (priority.HasFlag(IndexingPriority.Error) || priority.HasFlag(IndexingPriority.Disabled) || stats.IsInvalidIndex)
            {
                explain(indexName, () => string.Format("Cannot do dynamic queries on disabled index or index with errors (index name = {0})", indexName));
                return new DynamicQueryMatchResult(indexName, DynamicQueryMatchType.Failure);
            }

            var currentBestState = DynamicQueryMatchType.Complete;

            if (query.MapFields.All(x => definition.ContainsField(x.Name)) == false)
            {
                explain(indexName, () =>
                {
                    var missingFields = query.MapFields.Where(x => definition.ContainsField(x.Name) == false);
                    return $"The following fields are missing: {string.Join(", ", missingFields)}";
                });

                currentBestState = DynamicQueryMatchType.Partial;
            }

            //TODO arek: ignore highlighting for now

            foreach (var sortInfo in query.SortDescriptors) // with matching sort options
            {
                var sortField = sortInfo.Field;

                if (sortField.StartsWith(Constants.AlphaNumericFieldName) ||
                    sortField.StartsWith(Constants.RandomFieldName) ||
                    sortField.StartsWith(Constants.CustomSortFieldName))
                {
                    sortField = SortFieldHelper.CustomField(sortField).Name;
                }
                
                if (sortField.EndsWith("_Range"))
                    sortField = sortField.Substring(0, sortField.Length - "_Range".Length);

                IndexField autoIndexField;
                // if the field is not in the output, then we can't sort on it. 
                if (definition.ContainsField(sortField) == false)
                {
                    // for map-reduce queries try to get field from group by fields as well
                    if (query.IsMapReduce == false || ((AutoMapReduceIndexDefinition)definition).GroupByFields
                                                        .TryGetValue(sortField, out autoIndexField) == false)
                    {
                        explain(indexName, () => $"Rejected because index does not contains field '{sortField}' which we need to sort on");
                        currentBestState = DynamicQueryMatchType.Partial;
                        continue;
                    }
                }
                else
                {
                    autoIndexField = definition.GetField(sortField);
                }

                if (sortInfo.FieldType != autoIndexField.SortOption)
                {
                    if (autoIndexField.SortOption == null)
                    {
                        switch (sortInfo.FieldType) // if field is not sorted, we check if we asked for the default sorting
                        {
                            case SortOptions.String:
                            case SortOptions.None:
                                continue;
                        }
                    }

                    explain(indexName, () =>
                            $"The specified sort type ({sortInfo.FieldType}) is different than the one specified for field '{sortField}' ({autoIndexField.SortOption})");
                    return new DynamicQueryMatchResult(indexName, DynamicQueryMatchType.Failure);
                }
            }

            if (currentBestState == DynamicQueryMatchType.Complete && priority.HasFlag(IndexingPriority.Idle))
            {
                currentBestState = DynamicQueryMatchType.Partial;
                explain(indexName, () => $"The index (name = {indexName}) is disabled or abandoned. The preference is for active indexes - making a partial match");
            }

            if (currentBestState != DynamicQueryMatchType.Failure && query.IsMapReduce)
            {
                if (AssertMapReduceFields(query, (AutoMapReduceIndexDefinition)definition, currentBestState, explain) == false)
                {
                    return new DynamicQueryMatchResult(indexName, DynamicQueryMatchType.Failure);
                }
            }

            return new DynamicQueryMatchResult(indexName, currentBestState)
            {
                LastMappedEtag = index.GetLastMappedEtagFor(collection),
                NumberOfMappedFields = definition.MapFields.Count
            };
        }

        private bool AssertMapReduceFields(DynamicQueryMapping query, AutoMapReduceIndexDefinition definition, DynamicQueryMatchType currentBestState, ExplainDelegate explain)
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
                    explain(indexName, () => $"The following field {field.Name} has {field.MapReduceOperation} operation defined, while query required {mapField.MapReduceOperation}");

                    return false;
                }
            }
            
            if (query.GroupByFields.All(definition.GroupByFields.ContainsKey) == false)
            {
                explain(indexName, () =>
                {
                    var missingFields = query.GroupByFields.Where(x => definition.GroupByFields.ContainsKey(x) == false);
                    return $"The following group by fields are missing: {string.Join(", ", missingFields)}";
                });

                return false;
            }

            if (query.GroupByFields.Length != definition.GroupByFields.Count)
            {
                explain(indexName, () =>
                {
                    var extraFields = definition.GroupByFields.Where(x => query.GroupByFields.Contains(x.Key) == false);
                    return $"Index {indexName} has additional group by fields: {string.Join(", ", extraFields)}";
                });

                return false;
            }
            
            return true;
        }
    }
}