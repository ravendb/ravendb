using System.Collections.Generic;
using System.Linq;

namespace Raven.Server.Documents.ETL.Providers.ElasticSearch
{
    public static class ElasticSearchIndexValidator
    {
        private static int MaxIndexNameBytesLength = 255;

        private static readonly List<char> NotAllowedIndexNameCharacters = new List<char>()
        {
            '/',
            '\\',
            '*',
            '?',
            '"',
            '<',
            '>',
            '|',
            '\'',
            ',',
            '#',
            ':'
        };

        private static readonly List<char> NotAllowedIndexNameStartCharacters = new List<char>() { '-', '_', '+' };

        public static bool IsValidIndexName(string name, out string errorMessage)
        {
            if (string.IsNullOrWhiteSpace(name))
            {
                errorMessage = "Index name cannot be empty.";
                return false;
            }

            if (name.Any(char.IsUpper))
            {
                errorMessage = "Only lowercase are allowed.";
                return false;
            }

            if (NotAllowedIndexNameStartCharacters.Contains(name[0]))
            {
                var notAllowedStartCharacters = $"('{string.Join("', '", NotAllowedIndexNameStartCharacters)}')";
                errorMessage = $"Index name cannot start with {notAllowedStartCharacters}";
                return false;
            }

            if (IsValidCharactersUsed(name) == false)
            {
                var notAllowedCharacters = $"('{string.Join("', '", NotAllowedIndexNameCharacters)}')";
                errorMessage = $"Characters {notAllowedCharacters} are not allowed.";
                return false;
            }

            if (name.Length * sizeof(char) > MaxIndexNameBytesLength)
            {
                errorMessage = $"Index name cannot be longer than {MaxIndexNameBytesLength} bytes.";
                return false;
            }

            errorMessage = null;
            return true;
        }

        private static bool IsValidCharactersUsed(string name)
        {
            foreach (char c in name)
            {
                if (char.IsLetterOrDigit(c) == false && NotAllowedIndexNameCharacters.Contains(c))
                {
                    return false;
                }
            }

            return true;
        }
    }
}
