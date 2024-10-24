﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Server.Documents.Queries;
using Raven.Server.Extensions;
using Sparrow;

namespace Raven.Server.Documents.Indexes
{
    public sealed class AutoIndexNameFinder
    {
        public const string AutoIndexPrefix = "Auto/";

        public static string FindMapIndexName(string collection, IReadOnlyCollection<AutoIndexField> fields)
        {
            return FindName(collection, fields, isMapReduce: false);
        }

        public static string FindMapReduceIndexName(string collection, IReadOnlyCollection<AutoIndexField> fields,
            IReadOnlyCollection<AutoIndexField> groupBy)
        {
            if (groupBy == null)
                throw new ArgumentNullException(nameof(groupBy));

            var reducedByFields = string.Join("And", groupBy.Select(GetName).OrderBy(x => x, StringComparer.Ordinal));

            return $"{FindName(collection, fields, isMapReduce: true)}ReducedBy{reducedByFields}";
        }

        private static string FindName(string collection, IReadOnlyCollection<AutoIndexField> fields, bool isMapReduce)
        {
            if (string.IsNullOrWhiteSpace(collection))
                throw new ArgumentNullException(nameof(collection));

            if (fields == null)
                throw new ArgumentNullException(nameof(fields));

            collection = 
                string.Equals(collection, Constants.Documents.Collections.AllDocumentsCollection, StringComparison.OrdinalIgnoreCase) 
                    ? AbstractQueryRunner.AllDocsCollectionName : collection;

            if (fields.Count == 0)
            {
                var collectionOnly = $"{AutoIndexPrefix}{collection}";

                if (isMapReduce == false)
                    return $"{collectionOnly}/By{Constants.Documents.Indexing.Fields.DocumentIdFieldName.ToUpperFirstLetter()}";

                return collectionOnly;
            }
            
            var combinedFields = string.Join("And", fields.Select(GetName).OrderBy(x => x, StringComparer.Ordinal));

            string formattableString = $"{AutoIndexPrefix}{collection}/By{combinedFields}";
            if (formattableString.Length > 256)
            {
                var shorterString = formattableString.Substring(0, 256) + "..." +
                                    Hashing.XXHash64.Calculate(formattableString, Encoding.UTF8);
                return shorterString;

            }
            return formattableString;
        }

        private static string GetName(AutoIndexField x)
        {
            var name = x.Name;

            if (x.HasQuotedName)
                name = $"'{name}'";

            if (x.GroupByArrayBehavior == GroupByArrayBehavior.ByContent)
                name = AutoIndexField.GetGroupByArrayContentAutoIndexFieldName(name).ToUpperFirstLetter();

            if (x.Indexing is AutoFieldIndexing.Default or AutoFieldIndexing.No)
            {
                if (x.Spatial != null || x.Vector != null)
                    return name
                        .Replace(",", "|")
                        .Replace(" ", string.Empty)
                        .ToUpperFirstLetter();
                
                if (x.HasSuggestions)
                    return AutoIndexField.GetSuggestionsAutoIndexFieldName(name).ToUpperFirstLetter();

                return name;
            }

            var functions = new List<string>();

            if (x.Indexing.HasFlag(AutoFieldIndexing.Search))
                functions.Add(AutoIndexField.GetSearchAutoIndexFieldName(name).ToUpperFirstLetter());


            if (x.Indexing.HasFlag(AutoFieldIndexing.Highlighting))
                functions.Add(AutoIndexField.GetHighlightingAutoIndexFieldName(name).ToUpperFirstLetter());

            if (x.Indexing.HasFlag(AutoFieldIndexing.Exact))
                functions.Add(AutoIndexField.GetExactAutoIndexFieldName(name).ToUpperFirstLetter());

            if (x.HasSuggestions)
                functions.Add(AutoIndexField.GetSuggestionsAutoIndexFieldName(name).ToUpperFirstLetter());

            return string.Join("And", functions);
        }
    }
}
