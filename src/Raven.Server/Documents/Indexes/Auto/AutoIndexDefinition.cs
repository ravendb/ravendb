using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;

namespace Raven.Server.Documents.Indexes.Auto
{
    public class AutoIndexDefinition : IndexDefinitionBase
    {
        private readonly AutoIndexField[] _fields;
        private readonly Dictionary<string, AutoIndexField> _fieldsByName = new Dictionary<string, AutoIndexField>(); 

        private static readonly Regex ReplaceInvalidCharacterForFields = new Regex(@"[^\w_]", RegexOptions.Compiled);

        public AutoIndexDefinition(string collection, AutoIndexField[] fields)
            : base(FindIndexName(collection, fields), new[] { collection })
        {
            if (string.IsNullOrEmpty(collection))
                throw new ArgumentNullException(nameof(collection));

            if (fields == null)
                throw new ArgumentNullException(nameof(fields));

            if (fields.Length == 0)
                throw new ArgumentException("You must specify at least one field.", nameof(fields));

            _fields = fields;

            _fieldsByName = _fields.ToDictionary(x => x.Name, x => x);
        }

        public int CountOfMapFields => _fields.Length;

        public bool ContainsField(string field)
        {
            if (field.EndsWith("_Range"))
                field = field.Substring(0, field.Length - 6);

            return _fieldsByName.ContainsKey(field);
        }

        public AutoIndexField GetField(string field)
        {
            if (field.EndsWith("_Range"))
                field = field.Substring(0, field.Length - 6);

            return _fieldsByName[field];
        }

        private static string FindIndexName(string collection, IReadOnlyCollection<AutoIndexField> fields)
        {
            var combinedFields = string.Join("And", fields.Select(x => ReplaceInvalidCharacterForFields.Replace(x.Name, "_")).OrderBy(x => x));

            var sortOptions = fields.Where(x => x.SortOption != null).Select(x => x.Name).ToArray();
            if (sortOptions.Length > 0)
            {
                combinedFields = $"{combinedFields}SortBy{string.Join(string.Empty, sortOptions.OrderBy(x => x))}";
            }

            var highlighted = fields.Where(x => x.Highlighted).Select(x => x.Name).ToArray();
            if (highlighted.Length > 0)
            {
                combinedFields = $"{combinedFields}Highlight{string.Join("", highlighted.OrderBy(x => x))}";
            }

            return fields.Count == 0 ? $"Auto/{collection}" : $"Auto/{collection}/By{combinedFields}";
        }
    }
}