using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Client.Extensions;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Indexes.Auto;
using Raven.Server.Documents.Indexes.MapReduce.Auto;
using Raven.Server.Documents.Queries.Parser;
using Raven.Server.Documents.Queries.Sorting;

namespace Raven.Server.Documents.Queries.Dynamic
{
    public class DynamicQueryMapping
    {
        private static readonly CompareInfo InvariantCompare = CultureInfo.InvariantCulture.CompareInfo;

        public string ForCollection { get; private set; }

        public DynamicSortInfo[] SortDescriptors { get; private set; } = new DynamicSortInfo[0];

        public DynamicQueryMappingItem[] MapFields { get; private set; } = new DynamicQueryMappingItem[0];

        public string[] GroupByFields { get; private set; } = new string[0];

        public string[] HighlightedFields { get; private set; }

        public bool IsMapReduce { get; private set; }

        public List<Index> SupercededIndexes;

        public IndexDefinitionBase CreateAutoIndexDefinition()
        {
            if (IsMapReduce == false)
            {
                return new AutoMapIndexDefinition(ForCollection, MapFields.Select(field =>
                    new IndexField
                    {
                        Name = field.Name,
                        Storage = FieldStorage.No,
                        Sort = SortDescriptors.FirstOrDefault(x => field.Name.Equals(x.Name))?.FieldType
                    }).ToArray());
            }

            if (MapFields.Length == 0)
                throw new InvalidOperationException("Invalid dynamic map-reduce query mapping. There is no aggregation specified.");

            if (GroupByFields.Length == 0)
                throw new InvalidOperationException("Invalid dynamic map-reduce query mapping. There is no group by field specified.");

            return new AutoMapReduceIndexDefinition(ForCollection, MapFields.Select(field =>
                    new IndexField
                    {
                        Name = field.Name,
                        Storage = FieldStorage.Yes,
                        Aggregation = field.AggregationOperation,
                        Sort = SortDescriptors.FirstOrDefault(x => field.Name.Equals(x.Name))?.FieldType,
                    }).ToArray(),
                    GroupByFields.Select(field =>
                    new IndexField
                    {
                        Name = field,
                        Storage = FieldStorage.Yes,
                        Sort = SortDescriptors.FirstOrDefault(x => field.Equals(x.Name))?.FieldType,
                    }).ToArray());
        }

        public void ExtendMappingBasedOn(IndexDefinitionBase definitionOfExistingIndex)
        {
            Debug.Assert(definitionOfExistingIndex is AutoMapIndexDefinition || definitionOfExistingIndex is AutoMapReduceIndexDefinition, "We can only support auto-indexes.");

            var extendedMapFields = new List<DynamicQueryMappingItem>(MapFields);
            var extendedSortDescriptors = new List<DynamicSortInfo>(SortDescriptors);

            foreach (var field in definitionOfExistingIndex.MapFields.Values)
            {
                if (extendedMapFields.Any(x => x.Name.Equals(field.Name, StringComparison.OrdinalIgnoreCase)) == false)
                {
                    extendedMapFields.Add(new DynamicQueryMappingItem(field.Name, field.Aggregation));
                }

                if (extendedSortDescriptors.Any(x => x.Name.Equals(field.Name, StringComparison.OrdinalIgnoreCase)) == false && field.Sort != null)
                {
                    extendedSortDescriptors.Add(new DynamicSortInfo
                    {
                        Name = field.Name,
                        FieldType = field.Sort.Value
                    });
                }
            }

            //TODO arek - HighlightedFields

            MapFields = extendedMapFields.ToArray();
            SortDescriptors = extendedSortDescriptors.ToArray();
        }

        public static DynamicQueryMapping Create(IndexQueryServerSide query)
        {
            var result = new DynamicQueryMapping
            {
                ForCollection = query.Metadata.CollectionName
            };

            var fields = new Dictionary<string, DynamicQueryMappingItem>();
            var sorting = new Dictionary<string, DynamicSortInfo>();

            foreach (var field in query.Metadata.Fields)
                AddField(field.Key, field.Value);

            void AddField(string fieldName, ValueTokenType valueType)
            {
                if (fieldName == Constants.Documents.Indexing.Fields.DocumentIdFieldName)
                    return;

                fields[fieldName] = new DynamicQueryMappingItem(fieldName, AggregationOperation.None);

                switch (valueType)
                {
                    case ValueTokenType.Double:
                    case ValueTokenType.Long:
                        {
                            if (fieldName == Constants.Documents.Indexing.Fields.IndexFieldScoreName)
                                return;

                            if (InvariantCompare.IsPrefix(fieldName, Constants.Documents.Indexing.Fields.RandomFieldName, CompareOptions.None))
                                return;

                            sorting[fieldName] = (new DynamicSortInfo()
                            {
                                Name = fieldName,
                                FieldType = SortOptions.Numeric
                            });

                            break;
                        }
                }
            }

            if (query.Metadata.OrderBy != null)
            {
                foreach (var field in query.Metadata.OrderBy)
                {
                    var fieldName = field.Name;

                    if (fieldName == Constants.Documents.Indexing.Fields.IndexFieldScoreName)
                        continue;

                    if (fieldName.StartsWith(Constants.Documents.Indexing.Fields.RandomFieldName) ||
                        fieldName.StartsWith(Constants.Documents.Indexing.Fields.CustomSortFieldName))
                        continue;

                    if (sorting.TryGetValue(fieldName, out var existingSort) == false)
                    {
                        sorting[field.Name] = new DynamicSortInfo()
                        {
                            FieldType = GetSortType(field.OrderingType),
                            Name = fieldName
                        };
                    }
                    else
                    {
                        // sorting was set based on the type of variable in WHERE

                        if (field.OrderingType != OrderByFieldType.Implicit)
                        {
                            // but ORDER BY ... AS ... was set explicitly
                            existingSort.FieldType = GetSortType(field.OrderingType);
                        }
                    }

                    fields[field.Name] = new DynamicQueryMappingItem(fieldName, AggregationOperation.None);
                }
            }

            if (query.Metadata.IsGroupBy)
            {
                result.IsMapReduce = true;

                foreach (var selectField in query.Metadata.SelectFields)
                {
                    if (fields.TryGetValue(selectField.Name, out var existingField) == false)
                    {
                        if (selectField.AggregationOperation != AggregationOperation.None)
                            fields[selectField.Name] = new DynamicQueryMappingItem(selectField.Name, selectField.AggregationOperation);
                    }
                    else if (selectField.AggregationOperation != AggregationOperation.None)
                    {
                        existingField.AggregationOperation = selectField.AggregationOperation;
                    }
                }

                result.GroupByFields = query.Metadata.GroupBy;
            }

            // TODO arek - get rid of linq
            result.MapFields = fields.Values
                .OrderByDescending(x => x.Name.Length)
                .ToArray();

            result.SortDescriptors = sorting.Values.ToArray();

            result.HighlightedFields = query.HighlightedFields.EmptyIfNull().Select(x => x.Field).ToArray();

            return result;
        }

        private static SortOptions GetSortType(OrderByFieldType ordering)
        {
            switch (ordering)
            {
                case OrderByFieldType.Implicit:
                case OrderByFieldType.String:
                    return SortOptions.String;
                case OrderByFieldType.Long:
                case OrderByFieldType.Double:
                    return SortOptions.Numeric;
                case OrderByFieldType.AlphaNumeric:
                    return SortOptions.AlphaNumeric;
                default:
                    throw new ArgumentException(ordering.ToString());
            }
        }
    }
}