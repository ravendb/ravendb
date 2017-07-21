using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Sparrow;

namespace Raven.Server.Documents.Indexes
{
    public class IndexNameFinder
    {
        public static string FindMapIndexName(string collection, IReadOnlyCollection<IndexField> fields)
        {
            return FindName(collection, fields);
        }

        public static string FindMapReduceIndexName(string collection, IReadOnlyCollection<IndexField> fields,
            IReadOnlyCollection<IndexField> groupBy)
        {
            if (groupBy == null)
                throw new ArgumentNullException(nameof(groupBy));

            var reducedByFields = string.Join("And", groupBy.Select(x => x.Name).OrderBy(x => x));

            return $"{FindName(collection, fields)}ReducedBy{reducedByFields}";
        }

        private static string FindName(string collection, IReadOnlyCollection<IndexField> fields)
        {
            if (string.IsNullOrWhiteSpace(collection))
                throw new ArgumentNullException(nameof(collection));

            if (fields == null)
                throw new ArgumentNullException(nameof(fields));

            collection = collection == Constants.Documents.Collections.AllDocumentsCollection ? "AllDocs" : collection;

            if (fields.Count == 0)
                return $"Auto/{collection}";
            
            var combinedFields = string.Join("And", fields.Select(x => x.Name).OrderBy(x => x));

            var sortOptions = fields.Where(x => x.Sort != null && x.Sort.Value != SortOptions.String && x.Sort.Value != SortOptions.None)
                .Select(x => x.Name)
                .ToArray();

            if (sortOptions.Length > 0)
            {
                combinedFields = $"{combinedFields}SortBy{string.Join(string.Empty, sortOptions.OrderBy(x => x))}";
            }

            /*
            var highlighted = fields.Where(x => x.Highlighted).Select(x => IndexField.ReplaceInvalidCharactersInFieldName(x.Name)).ToArray();
            if (highlighted.Length > 0)
            {
                combinedFields = $"{combinedFields}Highlight{string.Join(string.Empty, highlighted.OrderBy(x => x))}";
            }
            */

            string formattableString = $"Auto/{collection}/By{combinedFields}";
            if (formattableString.Length > 256)
            {
                var shorterString = formattableString.Substring(0, 256) + "..." +
                                    Hashing.XXHash64.Calculate(formattableString, Encoding.UTF8);
                return shorterString;

            }
            return formattableString;
        }
    }
}