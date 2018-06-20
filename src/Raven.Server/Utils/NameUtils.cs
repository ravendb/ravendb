using System.Text.RegularExpressions;

namespace Raven.Server.Utils
{
    internal static class NameUtils
    {
        public const string ValidResourceNameCharacters = @"([_\-\.]+)";
        public const string ValidIndexNameCharacters = @"([_\/\-\.]+)";

        private static readonly Regex ValidResourceNameCharactersRegex = new Regex(ValidResourceNameCharacters, RegexOptions.Compiled);
        private static readonly Regex ValidIndexNameCharactersRegex = new Regex(ValidIndexNameCharacters, RegexOptions.Compiled);
        
        public static bool IsValidResourceName(string name)
        {
            return IsValidName(name, ValidResourceNameCharactersRegex);
        }
        
        public static bool IsValidIndexName(string name)
        {
            return IsValidName(name, ValidIndexNameCharactersRegex);
        }
        
        private static bool IsValidName(string name, Regex regex)
        {
            foreach (char c in name)
            {
                if (char.IsLetterOrDigit(c) == false && regex.Matches(c.ToString()).Count == 0)
                {
                    return false;
                }
            }
            
            return true;
        }
    }
    
    public class NameValidation
    {
        public bool IsValid { get; set; } 
        public string ErrorMessage { get; set; }
    }
}
