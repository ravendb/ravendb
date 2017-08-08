using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using Raven.Client;
using Raven.Client.Documents.Indexes;
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

        public bool IsMapReduce { get; private set; }

        public List<Index> SupercededIndexes;

        public IndexDefinitionBase CreateAutoIndexDefinition()
        {
            if (IsMapReduce == false)
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
                    extendedMapFields.Add(new DynamicQueryMappingItem(field.Name, field.Aggregation));
                }
            }

            //TODO arek - HighlightedFields

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

                var mapping = new DynamicQueryMappingItem(field, AggregationOperation.None);

                if (query.Metadata.WhereFields.TryGetValue(field, out var whereField) && whereField.IsFullTextSearch)
                    mapping.IsFullTextSearch = true;

                mapFields[field] = mapping;
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

                    mapFields[field.Name] = new DynamicQueryMappingItem(fieldName, AggregationOperation.None);
                }
            }

            if (query.Metadata.IsGroupBy)
            {
                result.IsMapReduce = true;

                var groupByFields = query.Metadata.GroupBy;
                
                if (query.Metadata.SelectFields != null)
                {
                    foreach (var field in query.Metadata.SelectFields)
                    {
                        if (field.IsGroupByKey == false)
                        {
                            var fieldName = field.Name;

                            if (mapFields.TryGetValue(fieldName, out var existingField) == false)
                            {
                                switch (field.AggregationOperation)
                                {
                                    case AggregationOperation.None:
                                        break;
                                    case AggregationOperation.Count:
                                    case AggregationOperation.Sum:
                                        mapFields[fieldName] = new DynamicQueryMappingItem(fieldName, field.AggregationOperation);
                                        break;
                                    default:
                                        ThrowUnknownAggregationOperation(field.AggregationOperation);
                                        break;
                                }
                            }
                            else if (field.AggregationOperation != AggregationOperation.None)
                            {
                                existingField.AggregationOperation = field.AggregationOperation;
                            }
                            else
                            {
                                Debug.Assert(groupByFields.Contains(fieldName));
                                // the field was specified in GROUP BY and WHERE
                                // let's remove it since GROUP BY fields are passed separately

                                mapFields.Remove(fieldName);
                            }
                        }
                        else
                        {
                            foreach (var groupBy in field.GroupByKeys)
                            {
                                mapFields.Remove(groupBy);
                            }
                        }
                    }
                }

                result.GroupByFields = new DynamicQueryMappingItem[groupByFields.Length];

                for (int i = 0; i < groupByFields.Length; i++)
                {
                    var groupByField = groupByFields[i];
                    
                    var mapping = new DynamicQueryMappingItem(groupByField, AggregationOperation.None);
                    
                    if (query.Metadata.WhereFields.TryGetValue(groupByField, out var whereField) && whereField.IsFullTextSearch)
                        mapping.IsFullTextSearch = true;
                    
                    result.GroupByFields[i] = mapping;
                }
            }

            result.MapFields = mapFields.Values.ToArray();

            result.HighlightedFields = query.HighlightedFields.EmptyIfNull().Select(x => x.Field).ToArray();

            return result;
        }

        private static void ThrowUnknownAggregationOperation(AggregationOperation operation)
        {
            throw new InvalidOperationException($"Unknown aggregation operation defined: {operation}");
        }
    }
}
