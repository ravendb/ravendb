using System;
using System.Collections.Generic;
using System.Linq;

using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Indexes.Auto;
using Raven.Server.Documents.Queries.Parse;
using Raven.Server.Documents.Queries.Sort;

namespace Raven.Server.Documents.Queries.Dynamic
{
    public class DynamicQueryMapping
    {
        public string ForCollection { get; private set; }

        public DynamicSortInfo[] SortDescriptors { get; private set; } = new DynamicSortInfo[0];

        public DynamicQueryMappingItem[] MapFields { get; private set; } = new DynamicQueryMappingItem[0];

        public string[] HighlightedFields { get; private set; }

        private DynamicQueryMapping()
        {
        }

        public AutoIndexDefinition CreateAutoIndexDefinition()
        {
            return new AutoIndexDefinition(ForCollection, MapFields.Select(field =>
                new IndexField
                {
                    Name = field.From,
                    Storage = FieldStorage.No,
                    SortOption = SortDescriptors.FirstOrDefault(x => field.To.Equals(x.Field))?.FieldType,
                    Highlighted = HighlightedFields.Any(x => field.To.Equals(x))
                }).ToArray());
        }

        public static DynamicQueryMapping Create(string entityName, IndexQuery query)
        {
            var fields = SimpleQueryParser.GetFieldsForDynamicQuery(query); // TODO arek - not sure if we really need a Tuple<string, string> here

            if (query.SortedFields != null)
            {
                foreach (var sortedField in query.SortedFields)
                {
                    var field = sortedField.Field;

                    if (field == Constants.TemporaryScoreValue)
                        continue;

                    if (field.StartsWith(Constants.AlphaNumericFieldName) ||
                        field.StartsWith(Constants.RandomFieldName) ||
                        field.StartsWith(Constants.CustomSortFieldName))
                    {
                        field = SortFieldHelper.CustomField(field).Name;
                    }

                    if (field.EndsWith("_Range"))
                        field = field.Substring(0, field.Length - "_Range".Length);

                    fields.Add(Tuple.Create(SimpleQueryParser.TranslateField(field), field));
                }
            }

            var dynamicQueryMapping = new DynamicQueryMapping
            {
                ForCollection = entityName,
                HighlightedFields = query.HighlightedFields.EmptyIfNull().Select(x => x.Field).ToArray(),
                SortDescriptors = GetSortInfo(fieldName =>
                {
                    if (fields.Any(x => x.Item2 == fieldName || x.Item2 == (fieldName + "_Range")) == false)
                        fields.Add(Tuple.Create(fieldName, fieldName));
                }, query)
            };

            dynamicQueryMapping.SetupFieldsToIndex(fields);
            dynamicQueryMapping.SetupSortDescriptors(dynamicQueryMapping.SortDescriptors);

            return dynamicQueryMapping;
        }

        private void SetupSortDescriptors(DynamicSortInfo[] sortDescriptors)
        {
            foreach (var dynamicSortInfo in sortDescriptors)
            {
                dynamicSortInfo.Field = IndexField.ReplaceInvalidCharactersInFieldName(dynamicSortInfo.Field);
            }
        }

        private void SetupFieldsToIndex(IEnumerable<Tuple<string, string>> fields)
        {
            MapFields = fields.Select(x => new DynamicQueryMappingItem
            {
                From = x.Item1,
                To = IndexField.ReplaceInvalidCharactersInFieldName(x.Item2),
                QueryFrom = EscapeParentheses(x.Item2)
            }).OrderByDescending(x => x.QueryFrom.Length).ToArray();

        }

        private string EscapeParentheses(string str)
        {
            return str.Replace("(", @"\(").Replace(")", @"\)");
        }

        public static DynamicSortInfo[] GetSortInfo(Action<string> addField, IndexQuery indexQuery)
        {
            var sortInfo = new List<DynamicSortInfo>();
            if (indexQuery.SortedFields == null)
                return new DynamicSortInfo[0];

            foreach (var sortOptions in indexQuery.SortedFields)
            {
                var key = sortOptions.Field;

                if (key.EndsWith("_Range"))
                {
                    sortInfo.Add(new DynamicSortInfo
                    {
                        Field = key.Substring(0, key.Length - "_Range".Length),
                        FieldType = SortOptions.NumericDefault
                    });
                }
                else
                {
                    sortInfo.Add(new DynamicSortInfo
                    {
                        Field = key,
                        FieldType = SortOptions.String
                    });
                }
            }

            return sortInfo.ToArray();
        }
    }
}