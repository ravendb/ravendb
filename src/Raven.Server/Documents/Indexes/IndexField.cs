using System.Text.RegularExpressions;
using Raven.Abstractions.Indexing;

namespace Raven.Server.Documents.Indexes
{
    public class IndexField
    {
        private static readonly Regex ReplaceInvalidCharacterForFields = new Regex(@"[^\w_]", RegexOptions.Compiled);

        public string Name { get; set; }

        public SortOptions? SortOption { get; set; }

        public bool Highlighted { get; set; }

        public FieldStorage Storage { get; set; }

        public FieldIndexing Indexing { get; set; }

        public static string ReplaceInvalidCharactersInFieldName(string field)
        {
            //TODO: This is probably expensive, we can do better
            return ReplaceInvalidCharacterForFields.Replace(field, "_");
        }
    }
}