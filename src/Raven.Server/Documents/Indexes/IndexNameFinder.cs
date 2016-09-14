using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Abstractions.Data;

namespace Raven.Server.Documents.Indexes
{
    public class IndexNameFinder
    {
        public static string FindMapIndexName(string[] collections, IReadOnlyCollection<IndexField> fields)
        {
            return FindName(collections, fields);
        }

        public static string FindMapReduceIndexName(string[] collections, IReadOnlyCollection<IndexField> fields,
            IReadOnlyCollection<IndexField> groupBy)
        {
            if (groupBy == null)
                throw new ArgumentNullException(nameof(groupBy));

            var reducedByFields = string.Join("And", groupBy.Select(x => IndexField.ReplaceInvalidCharactersInFieldName(x.Name)).OrderBy(x => x));

            return $"{FindName(collections, fields)}ReducedBy{reducedByFields}";
        }

        private static string FindName(string[] collections, IReadOnlyCollection<IndexField> fields)
        {
            foreach (var collection in collections.Where(string.IsNullOrEmpty))
                throw new ArgumentNullException(nameof(collection));

            if (fields == null)
                throw new ArgumentNullException(nameof(fields));

            var joinedCollections = string.Join("And", collections.Select(x => x == Constants.Indexing.AllDocumentsCollection ? "AllDocs" : x));

            if (fields.Count == 0)
                return $"Auto/{joinedCollections}";
            
            var combinedFields = string.Join("And", fields.Select(x => IndexField.ReplaceInvalidCharactersInFieldName(x.Name)).OrderBy(x => x));

            var sortOptions = fields.Where(x => x.SortOption != null).Select(x => IndexField.ReplaceInvalidCharactersInFieldName(x.Name)).ToArray();
            if (sortOptions.Length > 0)
            {
                combinedFields = $"{combinedFields}SortBy{string.Join(string.Empty, sortOptions.OrderBy(x => x))}";
            }

            var highlighted = fields.Where(x => x.Highlighted).Select(x => IndexField.ReplaceInvalidCharactersInFieldName(x.Name)).ToArray();
            if (highlighted.Length > 0)
            {
                combinedFields = $"{combinedFields}Highlight{string.Join(string.Empty, highlighted.OrderBy(x => x))}";
            }

            return $"Auto/{joinedCollections}/By{combinedFields}";
        }
    }
}