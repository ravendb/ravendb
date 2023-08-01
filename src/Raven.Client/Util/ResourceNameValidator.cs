using System;
using System.Text.RegularExpressions;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;

namespace Raven.Client.Util
{
    internal static class ResourceNameValidator
    {
        public static List<string> AllowedResourceNameCharacters = new List<string>()
        {
            "_", @"\-", @"\."
        };

        public static List<string> AllowedIndexNameCharacters = new List<string>()
        {
            "_", @"\-", @"\.", @"\/"
        };

        public static string ValidResourceNameCharacters = $"([{string.Join("", AllowedResourceNameCharacters)}]+)";
        public static string ValidIndexNameCharacters = $"([{string.Join("", AllowedIndexNameCharacters)}]+)";

        private static readonly Regex ValidResourceNameCharactersRegex = new Regex(ValidResourceNameCharacters, RegexOptions.Compiled);
        private static readonly Regex ValidIndexNameCharactersRegex = new Regex(ValidIndexNameCharacters, RegexOptions.Compiled);
        private static readonly Regex NameStartsOrEndsWithDotOrContainsConsecutiveDotsRegex = new Regex(@"^\.|\.\.|\.$", RegexOptions.Compiled);
       
        public static bool IsValidIndexName(string name)
        {
            return IsValidName(name, ValidIndexNameCharactersRegex);
        }
        
        // this is called from 'Client'
        public static void AssertValidDatabaseName(string databaseName)
        {
            if (IsValidResourceName(databaseName, null, out string errMsg) == false)
            {
                throw new InvalidOperationException(errMsg);
            }
        }
        
        // this is called from 'Server'
        public static bool IsValidResourceName(string name, string dataDirectory, out string errorMessage)
        {
            if (string.IsNullOrWhiteSpace(name))
            {
                errorMessage = "Name cannot be null or empty.";
                return false;
            }
            if (IsValidName(name, ValidIndexNameCharactersRegex) == false)
            {
                var allowedCharacters = $"('{string.Join("', '", AllowedResourceNameCharacters.Select(Regex.Unescape))}')";
                errorMessage = $"The name '{name}' is not permitted. Only letters, digits and characters {allowedCharacters} are allowed.";
                return false;
            }
            if (name.Length > Constants.Documents.MaxDatabaseNameLength)
            {
                errorMessage = $"The name '{name}' exceeds '{Constants.Documents.MaxDatabaseNameLength}' characters!";
                return false;
            }
            if (name.IndexOfAny(Path.GetInvalidFileNameChars()) >= 0)
            {
                errorMessage = $"The name '{name}' contains characters that are forbidden for use!";
                return false;
            }
            if (Constants.Platform.Windows.ReservedFileNames.Any(x => string.Equals(x, name, StringComparison.OrdinalIgnoreCase)))
            {
                errorMessage = $"The name '{name}' is forbidden for use!";
                return false;
            }
            if (name.Contains(".") && IsDotCharSurroundedByOtherChars(name) == false)
            {
                errorMessage = $"The name '{name}' is not permitted. If a name contains '.' character then it must be surrounded by other allowed characters.";
                return false;
            }
            
            dataDirectory = dataDirectory ?? string.Empty;
            if (Path.Combine(dataDirectory, name).Length > Constants.Platform.Windows.MaxPath)
            {
                int maxfileNameLength = Constants.Platform.Windows.MaxPath - dataDirectory.Length;
                errorMessage = $"Invalid name! Name cannot exceed {maxfileNameLength} characters";
                return false;
            }
            if ((RuntimeInformation.IsOSPlatform(OSPlatform.Linux) || RuntimeInformation.IsOSPlatform(OSPlatform.OSX)) &&
                ((name.Length > Constants.Platform.Linux.MaxFileNameLength) ||
                (dataDirectory.Length + name.Length > Constants.Platform.Linux.MaxPath)))
            {
                int theoreticalMaxFileNameLength = Constants.Platform.Linux.MaxPath - dataDirectory.Length;
                int maxfileNameLength = theoreticalMaxFileNameLength > Constants.Platform.Linux.MaxFileNameLength ? Constants.Platform.Linux.MaxFileNameLength : theoreticalMaxFileNameLength;
                errorMessage = $"Invalid name! Name cannot exceed {maxfileNameLength} characters";
                return false;
            }

            errorMessage = null;
            return true;
        }
        
        public static bool IsValidFileName(string name, out string errorMessage)
        {
            if (name.IndexOfAny(Path.GetInvalidFileNameChars()) >= 0)
            {
                errorMessage = $"The name '{name}' contains characters that are forbidden for use!";
                return false;
            }

            errorMessage = null;
            return true;
        }
        
        public static bool IsDotCharSurroundedByOtherChars(string name)
        {
            return NameStartsOrEndsWithDotOrContainsConsecutiveDotsRegex.IsMatch(name) == false;
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
    
    internal sealed class NameValidation
    {
        public bool IsValid { get; set; } 
        public string ErrorMessage { get; set; }
    }
}
