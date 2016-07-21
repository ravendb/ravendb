//-----------------------------------------------------------------------
// <copyright file="DynamicQueryMapping.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Abstractions.Util;
using Raven.Database.Indexing;
using Raven.Database.Util;

namespace Raven.Database.Data
{
    public class DynamicQueryMapping
    {
        public string IndexName { get; set; }
        public string ForEntityName { get; set; }
        public DynamicSortInfo[] SortDescriptors { get; set; }
        public DynamicQueryMappingItem[] Items { get; set; }
        public string[] HighlightedFields { get; set; }

        private List<Action<IndexDefinition>> extraActionsToPerform = new List<Action<IndexDefinition>>();

        public DynamicQueryMapping()
        {
            Items = new DynamicQueryMappingItem[0];
            SortDescriptors = new DynamicSortInfo[0];
        }

        public IndexDefinition CreateIndexDefinition()
        {
            var fromClause = string.Empty;
            var realMappings = new HashSet<string>();

            if (!string.IsNullOrEmpty(ForEntityName))
            {
                fromClause = "from doc in docs." + ForEntityName;
            }
            else
            {
                fromClause = "from doc in docs";
            }

            bool containsNestedItems = false;

            foreach (var map in Items)
            {
                var currentDoc = "doc";
                var currentExpression = new StringBuilder();
                var mapFromClauses = new List<String>();
                int currentIndex = 0;
                bool nestedCollection = false;
                while (currentIndex < map.From.Length)
                {
                    char currentChar = map.From[currentIndex++];
                    switch (currentChar)
                    {
                        case ',':
                            containsNestedItems = true;

                            // doc.NewDoc.Items
                            String newDocumentSource = string.Format("{0}.{1}", currentDoc, currentExpression);

                            // docNewDocItemsItem
                            String newDoc = string.Format("{0}Item", newDocumentSource.Replace(".", ""));

                            // from docNewDocItemsItem in doc.NewDoc.Items
                            String docInclude = string.Format("from {0} in ((IEnumerable<dynamic>){1}).DefaultIfEmpty()", newDoc, newDocumentSource);
                            mapFromClauses.Add(docInclude);
                            nestedCollection = true;
                            // Start building the property again
                            currentExpression.Clear();

                            // And from this new doc
                            currentDoc = newDoc;

                            break;
                        default:
                            nestedCollection = false;
                            currentExpression.Append(currentChar);
                            break;
                    }
                }

                if (currentExpression.Length > 0 && currentExpression[0] != '[')
                {
                    currentExpression.Insert(0, '.');
                }

                var indexedMember = currentExpression.ToString().Replace("_Range", "");
                string rightHandSide;

                if (indexedMember.Length == 0 && nestedCollection == false)
                    rightHandSide = currentDoc;
                else if (mapFromClauses.Count > 0)
                    rightHandSide = String.Format("({0} select {1}{2}).ToArray()", String.Join("\n", mapFromClauses), currentDoc,
                        indexedMember);
                else rightHandSide = String.Format("{0}{1}", currentDoc, indexedMember);

                realMappings.Add(string.Format("{0} = {1}",
                    map.To.Replace("_Range", ""),
                    rightHandSide
                    ));
            }

            string mapDefinition;

            if (realMappings.Count == 1 && containsNestedItems == false)
                mapDefinition = string.Format("{0}\r\nselect new {{ {1} }}", fromClause, realMappings.First());
            else
                mapDefinition = string.Format("{0}\r\nselect new\r\n{{\r\n\t{1}\r\n}}", fromClause, string.Join(",\r\n\t", realMappings));

            mapDefinition = IndexPrettyPrinter.TryFormat(mapDefinition);

            var index = new IndexDefinition
            {
                Map = mapDefinition,
                InternalFieldsMapping = new Dictionary<string, string>()
            };

            foreach (var item in Items)
            {
                index.InternalFieldsMapping[item.To] = item.From;
            }

            foreach (var descriptor in SortDescriptors)
            {
                index.SortOptions[ToFieldName(descriptor.Field)] = descriptor.FieldType;
            }

            foreach (var field in HighlightedFields.EmptyIfNull())
            {
                index.Stores[field] = FieldStorage.Yes;
                index.Indexes[field] = FieldIndexing.Analyzed;
                index.TermVectors[field] = FieldTermVector.WithPositionsAndOffsets;
            }
            return index;
        }

        private string ToFieldName(string field)
        {
            var item = Items.FirstOrDefault(x => x.From == field);
            if (item == null)
                return field;
            return item.To;
        }

        public static DynamicQueryMapping Create(DocumentDatabase database, string query, string entityName)
        {
            return Create(database, new IndexQuery
            {
                Query = query
            }, entityName);
        }

        public static DynamicQueryMapping Create(DocumentDatabase database, IndexQuery query, string entityName)
        {
            var fields = SimpleQueryParser.GetFieldsForDynamicQuery(query);

            if (query.SortedFields != null)
            {
                foreach (var sortedField in query.SortedFields)
                {
                    var field = sortedField.Field;

                    if (field.StartsWith(Constants.RandomFieldName) ||
                        field.StartsWith(Constants.CustomSortFieldName) ||
                        field.StartsWith(Constants.TemporaryScoreValue))
                        continue;

                    if (field.StartsWith(Constants.AlphaNumericFieldName))
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
                ForEntityName = entityName,
                HighlightedFields = query.HighlightedFields.EmptyIfNull().Select(x => x.Field).ToArray(),
                SortDescriptors = GetSortInfo(fieldName =>
                {
                    if (fields.Any(x => x.Item2 == fieldName || x.Item2 == (fieldName + "_Range")) == false)
                        fields.Add(Tuple.Create(fieldName, fieldName));
                }, query)
            };
            dynamicQueryMapping.SetupFieldsToIndex(query, fields);
            dynamicQueryMapping.SetupSortDescriptors(dynamicQueryMapping.SortDescriptors);
            dynamicQueryMapping.FindIndexName(database, dynamicQueryMapping, query);
            return dynamicQueryMapping;
        }

        private void SetupSortDescriptors(DynamicSortInfo[] sortDescriptors)
        {
            foreach (var dynamicSortInfo in sortDescriptors)
            {
                dynamicSortInfo.Field = ReplaceInvalidCharactersForFields(dynamicSortInfo.Field);
            }
        }

        public void AddExistingIndexDefinition(IndexDefinition indexDefinition, DocumentDatabase database, IndexQuery query)
        {
            var existing = database.IndexDefinitionStorage.GetIndexDefinition(indexDefinition.Name);
            if (existing == null || existing.InternalFieldsMapping == null) return; // No biggy, it just means we'll have two small indexes and we'll do this again later

            this.Items = this.Items.Union(
                existing.InternalFieldsMapping
                   .Where(field => this.Items.All(item => item.To != field.Key) && !field.Key.StartsWith("__"))
                   .Select(field => new DynamicQueryMappingItem()
                   {
                       From = field.Value,
                       To = ReplaceInvalidCharactersForFields(field.Key),
                       QueryFrom = EscapeParentheses(field.Key)
                   })
           ).ToArray();

            this.SortDescriptors = this.SortDescriptors.Union(
                indexDefinition.SortOptions
                    .Where(option => this.SortDescriptors.All(desc => desc.Field != option.Key))
                    .Select(option => new DynamicSortInfo()
                    {
                        Field = option.Key,
                        FieldType = option.Value
                    })
                ).ToArray();

            foreach (var fieldStorage in existing.Stores)
            {
                KeyValuePair<string, FieldStorage> storage = fieldStorage;
                extraActionsToPerform.Add(def => def.Stores[storage.Key] = storage.Value);
            }

            foreach (var fieldIndex in existing.Indexes)
            {
                KeyValuePair<string, FieldIndexing> index = fieldIndex;
                extraActionsToPerform.Add(def => def.Indexes[index.Key] = index.Value);
            }

            foreach (var fieldTermVector in existing.TermVectors)
            {
                KeyValuePair<string, FieldTermVector> vector = fieldTermVector;
                extraActionsToPerform.Add(def => def.TermVectors[vector.Key] = vector.Value);
            }
            this.FindIndexName(database, this, query);
        }

        static readonly Regex replaceInvalidCharacterForFields = new Regex(@"[^\w_]", RegexOptions.Compiled);
        private void SetupFieldsToIndex(IndexQuery query, IEnumerable<Tuple<string, string>> fields)
        {
            Items = fields.Select(x => new DynamicQueryMappingItem
            {
                From = x.Item1,
                To = ReplaceInvalidCharactersForFields(x.Item2),
                QueryFrom = EscapeParentheses(x.Item2)
            }).OrderByDescending(x => x.QueryFrom.Length).ToArray();
            
        }

        private string EscapeParentheses(string str)
        {
            return str.Replace("(", @"\(").Replace(")", @"\)");
        }

        public static string ReplaceInvalidCharactersForFields(string field)
        {
            return replaceInvalidCharacterForFields.Replace(field, "_");
        }

        public static DynamicSortInfo[] GetSortInfo(Action<string> addField, IndexQuery indexQuery)
        {
            var sortInfo = new List<DynamicSortInfo>();
            if(indexQuery.SortHints == null)
                return new DynamicSortInfo[0];
            foreach (var sortOptions in indexQuery.SortHints)
            {
                var key = sortOptions.Key;
                var fieldName = 
                    key.EndsWith("_Range") ?
                          key.Substring("SortHint-".Length, key.Length - "SortHint-".Length - "_Range".Length)
                        : key.Substring("SortHint-".Length);
                sortInfo.Add(new DynamicSortInfo
                {
                    Field = fieldName,
                    FieldType = sortOptions.Value
                });
            }


            return sortInfo.ToArray();
        }

        private void FindIndexName(DocumentDatabase database, DynamicQueryMapping map, IndexQuery query)
        {
            var targetName = map.ForEntityName ?? "AllDocs";

            var combinedFields = String.Join("And",
                map.Items
                .OrderBy(x => x.To)
                .Select(x => x.To));
            var indexName = combinedFields;

            if (map.SortDescriptors != null && map.SortDescriptors.Length > 0)
            {
                indexName = string.Format("{0}SortBy{1}", indexName,
                                          String.Join("",
                                                      map.SortDescriptors
                                                          .Select(x => x.Field)
                                                          .OrderBy(x => x)));
            }
            if (map.HighlightedFields != null && map.HighlightedFields.Length > 0)
            {
                indexName = string.Format("{0}Highlight{1}", indexName,
                    string.Join("", map.HighlightedFields.OrderBy(x => x)));
            }
            string groupBy = null;

            if (database.Configuration.RunInUnreliableYetFastModeThatIsNotSuitableForProduction == false &&
                database.Configuration.RunInMemory == false)
            {
                indexName = IndexingUtil.FixupIndexName(indexName, database.Configuration.DataDirectory);
            }

            var permanentIndexName = indexName.Length == 0
                    ? string.Format("Auto/{0}{1}", targetName, groupBy)
                    : string.Format("Auto/{0}/By{1}{2}", targetName, indexName, groupBy);

            map.IndexName = permanentIndexName;
        }

        public class DynamicSortInfo
        {
            public string Field { get; set; }
            public SortOptions FieldType { get; set; }
        }
    }
}
