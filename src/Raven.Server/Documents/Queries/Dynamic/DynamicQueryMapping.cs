using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Client.Exceptions;
using Raven.Client.Extensions;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Indexes.Auto;
using Raven.Server.Documents.Indexes.MapReduce.Auto;
using Raven.Server.Documents.Queries.Parser;
namespace Raven.Server.Documents.Queries.Dynamic
{
    public class DynamicQueryMapping
    {
        public string ForCollection { get; private set; }

        public DynamicQueryMappingItem[] MapFields { get; private set; } = new DynamicQueryMappingItem[0];

        public DynamicQueryMappingItem[] GroupByFields { get; private set; } = new DynamicQueryMappingItem[0];

        public string[] HighlightedFields { get; private set; }

        public bool IsGroupBy { get; private set; }

        public List<Index> SupersededIndexes;

        public IndexDefinitionBase CreateAutoIndexDefinition()
        {
            if (IsGroupBy == false)
            {
                return new AutoMapIndexDefinition(ForCollection, MapFields.Select(field =>
                    {
                        var indexField = new IndexField
                        {
                            Name = field.Name,
                            Storage = FieldStorage.No
                        };

                        if (field.IsFullTextSearch)
                            indexField.Indexing = FieldIndexing.Analyzed;

                        return indexField;
                    }
                ).ToArray());
            }

            if (GroupByFields.Length == 0)
                throw new InvalidOperationException("Invalid dynamic map-reduce query mapping. There is no group by field specified.");

            return new AutoMapReduceIndexDefinition(ForCollection, MapFields.Select(field =>
                {
                    var indexField = new IndexField
                    {
                        Name = field.Name,
                        Storage = FieldStorage.No,
                        Aggregation = field.AggregationOperation
                    };

                    if (field.IsFullTextSearch)
                        indexField.Indexing = FieldIndexing.Analyzed;

                    return indexField;
                }).ToArray(),
                GroupByFields.Select(field =>
                {
                    var indexField = new IndexField
                    {
                        Name = field.Name,
                        Storage = FieldStorage.No
                    };

                    if (field.IsFullTextSearch)
                        indexField.Indexing = FieldIndexing.Analyzed;

                    return indexField;
                }).ToArray());
        }

        public void ExtendMappingBasedOn(IndexDefinitionBase definitionOfExistingIndex)
        {
            Debug.Assert(definitionOfExistingIndex is AutoMapIndexDefinition || definitionOfExistingIndex is AutoMapReduceIndexDefinition, "We can only support auto-indexes.");

            var extendedMapFields = new List<DynamicQueryMappingItem>(MapFields);

            foreach (var field in definitionOfExistingIndex.MapFields.Values)
            {
                if (extendedMapFields.Any(x => x.Name.Equals(field.Name, StringComparison.OrdinalIgnoreCase)) == false)
                {
                    extendedMapFields.Add(DynamicQueryMappingItem.Create(field.Name, field.Aggregation));
                }
            }

            MapFields = extendedMapFields.ToArray();
        }

        public static DynamicQueryMapping Create(IndexQueryServerSide query)
        {
            var result = new DynamicQueryMapping
            {
                ForCollection = query.Metadata.CollectionName
            };

            var mapFields = new Dictionary<string, DynamicQueryMappingItem>();

            foreach (var field in query.Metadata.IndexFieldNames)
            {
                if (field == Constants.Documents.Indexing.Fields.DocumentIdFieldName)
                    continue;

                mapFields[field] = DynamicQueryMappingItem.Create(field, AggregationOperation.None, query.Metadata.WhereFields);
            }

            if (query.Metadata.OrderBy != null)
            {
                foreach (var field in query.Metadata.OrderBy)
                {
                    if (field.OrderingType == OrderByFieldType.Random)
                        continue;

                    if (field.OrderingType == OrderByFieldType.Score)
                        continue;

                    var fieldName = field.Name;

                    if (fieldName.StartsWith(Constants.Documents.Indexing.Fields.CustomSortFieldName))
                        continue;

                    if (mapFields.ContainsKey(field.Name))
                        continue;

                    mapFields.Add(field.Name, DynamicQueryMappingItem.Create(fieldName));
                }
            }

            if (query.Metadata.IsGroupBy)
            {
                result.IsGroupBy = true;
                result.GroupByFields = CreateGroupByFields(query, mapFields);
            }

            result.MapFields = new DynamicQueryMappingItem[mapFields.Count];

            int index = 0;
            foreach (var field in mapFields)
            {
                if (result.IsGroupBy && field.Value.AggregationOperation == AggregationOperation.None)
                {
                    throw new InvalidQueryException($"Field '{field.Key}' isn't neither an aggregation operation nor part of the group by key", query.Metadata.QueryText,
                        query.QueryParameters);
                }

                result.MapFields[index++] = field.Value;
            }

            return result;
        }

        private static DynamicQueryMappingItem[] CreateGroupByFields(IndexQueryServerSide query, Dictionary<string, DynamicQueryMappingItem> mapFields)
        {
            var groupByFields = query.Metadata.GroupBy;

            if (query.Metadata.SelectFields != null)
            {
                foreach (var field in query.Metadata.SelectFields)
                {
                    if (field.IsGroupByKey)
                        continue;

                    var fieldName = field.Name;

                    if (mapFields.TryGetValue(fieldName, out var existingField) == false)
                    {
                        switch (field.AggregationOperation)
                        {
                            case AggregationOperation.None:
                                break;
                            case AggregationOperation.Count:
                            case AggregationOperation.Sum:
                                mapFields.Add(fieldName, DynamicQueryMappingItem.Create(fieldName, field.AggregationOperation));
                                break;
                            default:
                                ThrowUnknownAggregationOperation(field.AggregationOperation);
                                break;
                        }
                    }
                    else if (field.AggregationOperation != AggregationOperation.None)
                    {
                        existingField.SetAggregation(field.AggregationOperation);
                    }
                }
            }

            var result = new DynamicQueryMappingItem[groupByFields.Length];

            for (int i = 0; i < groupByFields.Length; i++)
            {
                var groupByField = groupByFields[i];

                result[i] = DynamicQueryMappingItem.Create(groupByField, AggregationOperation.None, query.Metadata.WhereFields);

                mapFields.Remove(groupByField); // ensure we don't have duplicated group by fields
            }

            return result;
        }

        private static void ThrowUnknownAggregationOperation(AggregationOperation operation)
        {
            throw new InvalidOperationException($"Unknown aggregation operation defined: {operation}");
        }
    }
}
