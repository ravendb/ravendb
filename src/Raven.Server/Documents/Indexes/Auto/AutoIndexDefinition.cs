using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;

namespace Raven.Server.Documents.Indexes.Auto
{
    public class AutoIndexDefinition : IndexDefinitionBase
    {
        private readonly AutoIndexField[] _fields;

        private static readonly Regex ReplaceInvalidCharacterForFields = new Regex(@"[^\w_]", RegexOptions.Compiled);

        public AutoIndexDefinition(string collection, AutoIndexField[] fields)
            : base(FindIndexName(collection, fields), new[] { collection })
        {
            _fields = fields;
        }

        public IEnumerable<string> MapFields => _fields.Select(x => x.Name);

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