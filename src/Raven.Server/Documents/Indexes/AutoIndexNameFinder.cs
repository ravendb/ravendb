using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Sparrow;

namespace Raven.Server.Documents.Indexes
{
    public class AutoIndexNameFinder
    {
        public static string FindMapIndexName(string collection, IReadOnlyCollection<AutoIndexField> fields)
        {
            return FindName(collection, fields);
        }

        public static string FindMapReduceIndexName(string collection, IReadOnlyCollection<AutoIndexField> fields,
            IReadOnlyCollection<AutoIndexField> groupBy)
        {
            if (groupBy == null)
                throw new ArgumentNullException(nameof(groupBy));

            var reducedByFields = string.Join("And", groupBy.Select(GetName).OrderBy(x => x));

            return $"{FindName(collection, fields)}ReducedBy{reducedByFields}";
        }

        private static string FindName(string collection, IReadOnlyCollection<AutoIndexField> fields)
        {
            if (string.IsNullOrWhiteSpace(collection))
                throw new ArgumentNullException(nameof(collection));

            if (fields == null)
                throw new ArgumentNullException(nameof(fields));

            collection = 
                string.Equals(collection, Constants.Documents.Collections.AllDocumentsCollection, StringComparison.OrdinalIgnoreCase) 
                    ? "AllDocs" : collection;

            if (fields.Count == 0)
                return $"Auto/{collection}";
            
            var combinedFields = string.Join("And", fields.Select(GetName).OrderBy(x => x));

            string formattableString = $"Auto/{collection}/By{combinedFields}";
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
            if (x.Indexing == AutoFieldIndexing.Default || x.Indexing == AutoFieldIndexing.No)
                return x.Name;

            var name = string.Empty;

            if (x.Indexing.HasFlag(AutoFieldIndexing.Search))
                name += CultureInfo.InvariantCulture.TextInfo.ToTitleCase(AutoIndexField.GetSearchAutoIndexFieldName(x.Name));

            if (x.Indexing.HasFlag(AutoFieldIndexing.Exact))
                name += CultureInfo.InvariantCulture.TextInfo.ToTitleCase(AutoIndexField.GetExactAutoIndexFieldName(x.Name));

            return name;
        }
    }
}
