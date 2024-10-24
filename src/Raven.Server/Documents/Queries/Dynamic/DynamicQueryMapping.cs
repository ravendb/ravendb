﻿using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Indexes.Spatial;
using Raven.Client.Documents.Indexes.Vector;
using Raven.Client.Exceptions;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Indexes.Auto;
using Raven.Server.Documents.Indexes.MapReduce.Auto;
using Raven.Server.Documents.Queries.AST;
using Index = Raven.Server.Documents.Indexes.Index;

namespace Raven.Server.Documents.Queries.Dynamic
{
    public sealed class DynamicQueryMapping
    {
        public string ForCollection { get; private set; }

        public Dictionary<string, DynamicQueryMappingItem> MapFields { get; private set; }

        public Dictionary<string, DynamicQueryMappingItem> GroupByFields { get; private set; }

        public List<string> GroupByFieldNames { get; private set; }

        public bool IsGroupBy { get; private set; }

        public List<Index> SupersededIndexes;

        internal AutoIndexDefinitionBaseServerSide CreateAutoIndexDefinition()
        {
            int idForFields = 1;
            if (IsGroupBy == false)
            {
                AutoIndexField[] autoIndexFields = MapFields.Values
                    .OrderBy(x => x.Name.Value, StringComparer.Ordinal)
                    .Select(field =>
                        {
                            var indexField = new AutoIndexField
                            {
                                Id = idForFields++,
                                Name = field.Name,
                                Storage = FieldStorage.No,
                                Indexing = AutoFieldIndexing.Default,
                                HasQuotedName = field.Name.IsQuoted
                            };

                            if (field.IsFullTextSearch)
                                indexField.Indexing |= AutoFieldIndexing.Search;

                            if (field.IsExactSearch)
                                indexField.Indexing |= AutoFieldIndexing.Exact;

                            if (field.HasHighlighting)
                                indexField.Indexing |= AutoFieldIndexing.Highlighting;
                            
                            if (field.Vector != null)
                                indexField.Vector = new AutoVectorOptions(field.Vector);

                            if (field.Spatial != null)
                                indexField.Spatial = new AutoSpatialOptions(field.Spatial);

                            indexField.HasSuggestions = field.HasSuggestions;

                            return indexField;
                        }
                    ).ToArray();

                var mapIndexName = AutoIndexNameFinder.FindMapIndexName(ForCollection, autoIndexFields);

                return new AutoMapIndexDefinition(mapIndexName, ForCollection, autoIndexFields, deploymentMode: null, clusterState: null);
            }

            if (GroupByFields.Count == 0)
                throw new InvalidOperationException("Invalid dynamic map-reduce query mapping. There is no group by field specified.");

            var mapFields = MapFields.Values.Select(field =>
            {
                var indexField = new AutoIndexField
                {
                    Id = idForFields++,
                    Name = field.Name,
                    Storage = FieldStorage.No,
                    Aggregation = field.AggregationOperation,
                    Indexing = AutoFieldIndexing.Default,
                    HasQuotedName = field.Name.IsQuoted
                };

                if (field.IsFullTextSearch)
                    indexField.Indexing |= AutoFieldIndexing.Search;

                if (field.IsExactSearch)
                    indexField.Indexing |= AutoFieldIndexing.Exact;

                if (field.HasHighlighting)
                    indexField.Indexing |= AutoFieldIndexing.Highlighting;

                if (field.Spatial != null)
                    indexField.Spatial = new AutoSpatialOptions(field.Spatial);
                
                if (field.Vector != null)
                    indexField.Vector = new AutoVectorOptions(field.Vector);

                indexField.HasSuggestions = field.HasSuggestions;

                return indexField;
            }).ToArray();

            var groupByFields = GroupByFields.Values
                .OrderBy(x => x.Name.Value, StringComparer.Ordinal)
                .Select(field =>
                {
                    var indexField = new AutoIndexField
                    {
                        Id = idForFields++,
                        Name = field.Name,
                        Storage = FieldStorage.No,
                        Indexing = AutoFieldIndexing.Default,
                        HasQuotedName = field.Name.IsQuoted,
                        GroupByArrayBehavior = field.GroupByArrayBehavior
                    };

                    if (field.IsFullTextSearch)
                        indexField.Indexing |= AutoFieldIndexing.Search;

                    if (field.IsExactSearch)
                        indexField.Indexing |= AutoFieldIndexing.Exact;

                    if (field.Spatial != null)
                        indexField.Spatial = new AutoSpatialOptions(field.Spatial);
                    
                    if (field.Vector != null)
                        indexField.Vector = new AutoVectorOptions(field.Vector);

                    indexField.HasSuggestions = field.HasSuggestions;

                    return indexField;
                }).ToArray();

            var mapReduceIndexName = AutoIndexNameFinder.FindMapReduceIndexName(ForCollection, mapFields, groupByFields);

            return new AutoMapReduceIndexDefinition(mapReduceIndexName, ForCollection, mapFields, groupByFields, GroupByFieldNames, deploymentMode: null, clusterState: null);
        }

        internal void ExtendMappingBasedOn(AutoIndexDefinitionBaseServerSide definitionOfExistingIndex)
        {
            Debug.Assert(definitionOfExistingIndex is AutoMapIndexDefinition || definitionOfExistingIndex is AutoMapReduceIndexDefinition, "Dynamic queries are handled only by auto indexes");

            switch (definitionOfExistingIndex)
            {
                case AutoMapIndexDefinition def:
                    Update(MapFields, def.MapFields, isGroupBy: false);
                    break;
                case AutoMapReduceIndexDefinition def:
                    Update(MapFields, def.MapFields, isGroupBy: false);
                    Update(GroupByFields, def.GroupByFields, isGroupBy: true);
                    break;
            }

            void Update<T>(Dictionary<string, DynamicQueryMappingItem> fields, Dictionary<string, T> indexFields, bool isGroupBy) where T : IndexFieldBase
            {
                foreach (var f in indexFields.Values)
                {
                    var indexField = f.As<AutoIndexField>();

                    if (fields.TryGetValue(indexField.Name, out var queryField))
                    {
                        var isFullTextSearch = queryField.IsFullTextSearch || indexField.Indexing.HasFlag(AutoFieldIndexing.Search);
                        var isExactSearch = queryField.IsExactSearch || indexField.Indexing.HasFlag(AutoFieldIndexing.Exact);

                        var field = isGroupBy == false
                            ? DynamicQueryMappingItem.Create(
                                queryField.Name,
                                queryField.AggregationOperation,
                                isFullTextSearch: isFullTextSearch,
                                isExactSearch: isExactSearch,
                                hasHighlighting: queryField.HasHighlighting || indexField.Indexing.HasFlag(AutoFieldIndexing.Highlighting),
                                hasSuggestions: queryField.HasSuggestions || indexField.HasSuggestions,
                                spatial: queryField.Spatial ?? indexField.Spatial,
                                vector: queryField.Vector ?? indexField.Vector)
                            : DynamicQueryMappingItem.CreateGroupBy(
                                queryField.Name,
                                queryField.GroupByArrayBehavior,
                                isSpecifiedInWhere: queryField.IsSpecifiedInWhere,
                                isFullTextSearch: isFullTextSearch,
                                isExactSearch: isExactSearch);

                        fields[queryField.Name] = field;
                    }
                    else
                    {
                        if (isGroupBy)
                            throw new InvalidOperationException("Cannot create new GroupBy field when extending mapping");

                        fields.Add(indexField.Name, DynamicQueryMappingItem.Create(
                            new QueryFieldName(indexField.Name, indexField.HasQuotedName),
                            indexField.Aggregation,
                            isFullTextSearch: indexField.Indexing.HasFlag(AutoFieldIndexing.Search),
                            isExactSearch: indexField.Indexing.HasFlag(AutoFieldIndexing.Exact),
                            hasHighlighting: indexField.Indexing.HasFlag(AutoFieldIndexing.Highlighting),
                            hasSuggestions: indexField.HasSuggestions,
                            spatial: indexField.Spatial,
                            vector: indexField.Vector));
                    }
                }
            }
        }

        public static DynamicQueryMapping Create(IndexQueryServerSide query)
        {
            var result = new DynamicQueryMapping
            {
                ForCollection = query.Metadata.CollectionName
            };

            var mapFields = new Dictionary<string, DynamicQueryMappingItem>(StringComparer.Ordinal);

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

                    if (field.Name == Constants.Documents.Indexing.Fields.DocumentIdFieldName)
                        continue;

                    var fieldName = field.Name;

#if FEATURE_CUSTOM_SORTING
                    if (fieldName.Value.StartsWith(Constants.Documents.Indexing.Fields.CustomSortFieldName))
                        continue;
#endif

                    if (mapFields.ContainsKey(field.Name))
                        continue;

                    mapFields.Add(field.Name, DynamicQueryMappingItem.Create(fieldName, field.AggregationOperation));
                }
            }

            if (query.Metadata.IsGroupBy)
            {
                result.IsGroupBy = true;
                result.GroupByFieldNames = query.Metadata.GroupBy.Select(x => x.Name.Value).ToList();
                result.GroupByFields = CreateGroupByFields(query, mapFields);

                foreach (var field in mapFields)
                {
                    if (field.Value.AggregationOperation == AggregationOperation.None)
                    {
                        throw new InvalidQueryException($"Field '{field.Key}' is neither an aggregation operation nor part of the group by key", query.Metadata.QueryText,
                            query.QueryParameters);
                    }
                }
            }

            if (query.Metadata.HasHighlightings)
            {
                foreach (var highlighting in query.Metadata.Highlightings)
                {
                    var fieldName = highlighting.Field;
                    if (mapFields.TryGetValue(fieldName, out var value))
                        value.HasHighlighting = true;
                    else
                        mapFields[fieldName] = DynamicQueryMappingItem.Create(fieldName, AggregationOperation.None, isFullTextSearch: true, isExactSearch: false, hasHighlighting: true, hasSuggestions: false, spatial: null, vector: null);
                }
            }

            if (query.Metadata.HasSuggest)
            {
                foreach (var selectField in query.Metadata.SelectFields)
                {
                    var fieldName = selectField.Name;
                    if (mapFields.TryGetValue(fieldName, out var value))
                        value.HasSuggestions = true;
                    else
                        mapFields[fieldName] = DynamicQueryMappingItem.Create(fieldName, AggregationOperation.None, isFullTextSearch: false, isExactSearch: false, hasHighlighting: false, hasSuggestions: true, spatial: null, vector: null);
                }
            }

            result.MapFields = mapFields;

            return result;
        }

        public static DynamicQueryMapping Create(Index index)
        {
            if (index.Type.IsAuto() == false)
                throw new InvalidOperationException();

            var mapping = new DynamicQueryMapping
            {
                ForCollection = index.Collections.First(),
                MapFields = new Dictionary<string, DynamicQueryMappingItem>(StringComparer.Ordinal)
            };

            var definition = (AutoIndexDefinitionBaseServerSide)index.Definition;

            foreach (var field in definition.MapFields)
            {
                var autoField = (AutoIndexField)field.Value;

                mapping.MapFields[field.Key] = DynamicQueryMappingItem.Create(
                            new QueryFieldName(autoField.Name, autoField.HasQuotedName),
                            autoField.Aggregation,
                            isFullTextSearch: autoField.Indexing.HasFlag(AutoFieldIndexing.Search),
                            isExactSearch: autoField.Indexing.HasFlag(AutoFieldIndexing.Exact),
                            hasHighlighting: autoField.Indexing.HasFlag(AutoFieldIndexing.Highlighting),
                            hasSuggestions: autoField.HasSuggestions,
                            spatial: autoField.Spatial,
                            vector: autoField.Vector);
            }

            if (index.Type.IsMapReduce())
            {
                mapping.IsGroupBy = true;

                var mapReduceDefinition = (AutoMapReduceIndexDefinition)definition;
                mapping.GroupByFields = new Dictionary<string, DynamicQueryMappingItem>(StringComparer.Ordinal);
                mapping.GroupByFieldNames = new List<string>();

                foreach (var field in mapReduceDefinition.GroupByFields)
                {
                    var autoField = field.Value;
                    mapping.GroupByFields[field.Key] = DynamicQueryMappingItem.CreateGroupBy(
                        new QueryFieldName(autoField.Name, autoField.HasQuotedName),
                        autoField.GroupByArrayBehavior,
                        isSpecifiedInWhere: true,
                        isFullTextSearch: autoField.Indexing.HasFlag(AutoFieldIndexing.Search),
                        isExactSearch: autoField.Indexing.HasFlag(AutoFieldIndexing.Exact));

                    mapping.GroupByFieldNames.Add(field.Key);
                }
            }

            return mapping;
        }

        private static Dictionary<string, DynamicQueryMappingItem> CreateGroupByFields(IndexQueryServerSide query, Dictionary<string, DynamicQueryMappingItem> mapFields)
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

            if (query.Metadata.CountInJs.HasValue)
            {
                if (mapFields.TryGetValue(Constants.Documents.Indexing.Fields.CountFieldName, out var countField) == false)
                {
                    mapFields.Add(Constants.Documents.Indexing.Fields.CountFieldName, DynamicQueryMappingItem.Create(QueryFieldName.Count, AggregationOperation.Count));
                }
            }

            if (query.Metadata.SumInJs is not null)
            {
                if (mapFields.TryGetValue(query.Metadata.SumInJs, out var sumField) == false)
                {
                    var queryFieldName = new QueryFieldName(query.Metadata.SumInJs, false);
                    mapFields.Add(query.Metadata.SumInJs, DynamicQueryMappingItem.Create(queryFieldName, AggregationOperation.Sum));
                }
            }

            var result = new Dictionary<string, DynamicQueryMappingItem>(groupByFields.Length, StringComparer.Ordinal);

            for (int i = 0; i < groupByFields.Length; i++)
            {
                var groupByField = groupByFields[i];

                result[groupByField.Name] = DynamicQueryMappingItem.CreateGroupBy(groupByField.Name, groupByField.GroupByArrayBehavior, query.Metadata.WhereFields);

                mapFields.Remove(groupByField.Name);  // ensure we don't have duplicated group by fields
            }

            return result;
        }

        [DoesNotReturn]
        private static void ThrowUnknownAggregationOperation(AggregationOperation operation)
        {
            throw new InvalidOperationException($"Unknown aggregation operation defined: {operation}");
        }
    }
}
