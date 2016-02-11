using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;

namespace Raven.Server.Indexes.Auto
{
    public class AutoIndexDefinition
    {
        private readonly AutoIndexField[] _fields;
        static readonly Regex ReplaceInvalidCharacterForFields = new Regex(@"[^\w_]", RegexOptions.Compiled);

        public AutoIndexDefinition(string collection, AutoIndexField[] fields)
        {
            _fields = fields;
            Collection = collection;
            Name = FindIndexName();
        }

        public string Collection { get; }

        public IEnumerable<string> MapFields => _fields.Select(x => x.Name);

        public string Name { get; }

        private string FindIndexName()
        {
            var combinedFields = string.Join("And", _fields.Select(x => ReplaceInvalidCharacterForFields.Replace(x.Name, "_")).OrderBy(x => x));

            var sortOptions = _fields.Where(x => x.SortOption != null).Select(x => x.Name).ToArray();
            if (sortOptions.Length > 0)
            {
                combinedFields = $"{combinedFields}SortBy{string.Join(string.Empty, sortOptions.OrderBy(x => x))}";
            }

            var highlighted = _fields.Where(x => x.Highlighted).Select(x => x.Name).ToArray();
            if (highlighted.Length > 0)
            {
                combinedFields = $"{combinedFields}Highlight{string.Join("", highlighted.OrderBy(x => x))}";
            }

            return _fields.Length == 0 ? $"Auto/{Collection}" : $"Auto/{Collection}/By{combinedFields}";
        }
    }
}