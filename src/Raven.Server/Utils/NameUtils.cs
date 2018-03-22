using System.Text.RegularExpressions;

namespace Raven.Server.Utils
{
    internal static class NameUtils
    {
        public const string ValidResourceNameCharacters = @"([A-Za-z0-9_\-\.]+)";

        public const string ValidIndexNameCharacters = @"([A-Za-z0-9_\/\-\.]+)";

        private static readonly Regex ValidIndexNameCharactersRegex = new Regex(ValidIndexNameCharacters, RegexOptions.Compiled);

        private static readonly Regex ValidResourceNameCharactersRegex = new Regex(ValidIndexNameCharacters, RegexOptions.Compiled);

        public static bool IsValidIndexName(string name)
        {
            var result = ValidIndexNameCharactersRegex.Matches(name);

            return result.Count != 0 && result[0].Value == name;
        }

        public static bool IsValidResourceName(string name)
        {
            var result = ValidResourceNameCharactersRegex.Matches(name);

            return result.Count != 0 && result[0].Value == name;
        }
    }
}
