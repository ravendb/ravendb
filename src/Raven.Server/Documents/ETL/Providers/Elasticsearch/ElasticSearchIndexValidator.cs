using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Raven.Server.Documents.ETL.Providers.ElasticSearch
{
    public static class ElasticSearchIndexValidator
    {
        private static int MaxIndexNameBytesLength = 255;

        private static readonly List<string> NotAllowedIndexNameCharacters = new()
        {
            "/",
            "\\",
            "*",
            "?",
            "\"",
            "<",
            ">",
            "|",
            " ",
            ",",
            "#",
            ":"
        };

        private static readonly List<string> NotAllowedIndexNameStartCharacters = new() { "-", "_", "+" };

        public static bool IsValidIndexName(string name, out string errorMessage)
        {
            if (string.IsNullOrWhiteSpace(name))
            {
                errorMessage = "Index name cannot be empty";
                return false;
            }

            if (name.Any(char.IsUpper))
            {
                errorMessage = "Index name must be lowercase";
                return false;
            }

            if (NotAllowedIndexNameStartCharacters.Any(name.StartsWith))
            {
                var notAllowedStartCharacters = $"('{string.Join("', '", NotAllowedIndexNameStartCharacters)}')";
                errorMessage = $"Index name cannot start with {notAllowedStartCharacters}";
                return false;
            }

            if (NotAllowedIndexNameCharacters.Any(name.Contains))
            {
                var notAllowedCharacters = $"('{string.Join("', '", NotAllowedIndexNameCharacters)}')";
                errorMessage = $"Characters {notAllowedCharacters} are not allowed";
                return false;
            }

            if (Encoding.UTF8.GetByteCount(name) > MaxIndexNameBytesLength)
            {
                errorMessage = $"Index name cannot be longer than {MaxIndexNameBytesLength} bytes";
                return false;
            }

            errorMessage = null;
            return true;
        }
    }
}
