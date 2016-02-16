using System.Text.RegularExpressions;
using Raven.Abstractions.Indexing;

namespace Raven.Server.Documents.Indexes
{
    public abstract class IndexField
    {
        private static readonly Regex ReplaceInvalidCharacterForFields = new Regex(@"[^\w_]", RegexOptions.Compiled);

        public string Name { get; protected set; }

        public SortOptions? SortOption { get; protected set; }

        public bool Highlighted { get; protected set; }

        public abstract FieldStorage Storage { get; }

        public abstract FieldIndexing Indexing { get; }

        public static string ReplaceInvalidCharactersInFieldName(string field)
        {
            return ReplaceInvalidCharacterForFields.Replace(field, "_");
        }
    }
}