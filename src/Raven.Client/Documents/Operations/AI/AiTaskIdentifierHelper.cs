using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Raven.Client.Documents.Operations.ETL;
using static Raven.Client.Constants;

namespace Raven.Client.Documents.Operations.AI;

internal static class AiTaskIdentifierHelper
{

    internal static bool ValidateIdentifier(string identifier, out List<string> errors)
    {
        errors = [];

        if (string.IsNullOrWhiteSpace(identifier))
        {
            errors.Add("Identifier cannot be empty or contain only whitespace;");
            return false;
        }

        // Check that the string is already normalized (contains only a-z, 0-9 and hyphens)
        if (identifier != identifier.Normalize(NormalizationForm.FormD))
            errors.Add("Identifier contains diacritical marks or non-ASCII characters;");

        // Check that there are no uppercase letters
        if (identifier.Any(char.IsUpper))
            errors.Add("Identifier contains uppercase letters;");

        // Check for invalid characters and collect them
        var invalidChars = identifier.Where(c => c is not (>= 'a' and <= 'z' or >= '0' and <= '9' or '-'))
            .Distinct()
            .ToList();
        if (invalidChars.Count != 0)
            errors.Add($"Identifier contains invalid characters: {string.Join(", ", invalidChars.Select(c => $"'{c}'"))}. " +
                       $"Only lowercase letters (a-z), numbers (0-9) and hyphens (-) are allowed.");

        // Check that there are no consecutive hyphens
        if (identifier.Contains("--"))
            errors.Add("Identifier contains consecutive hyphens;");

        // Check that the string does not end with a hyphen
        if (identifier.EndsWith("-"))
            errors.Add("Identifier ends with a hyphen;");

        return errors.Count == 0;
    }

    internal static string GenerateIdentifier(string input)
    {
        if (string.IsNullOrWhiteSpace(input))
            return null;

        var result = new StringBuilder();
        var lastWasHyphen = false;

        // First normalize to FormD to separate letters from their diacritics
        foreach (var c in input.Normalize(NormalizationForm.FormD))
        {
            // Check if this is a letter that needs to be preserved
            if (c is
                >= 'a' and <= 'z' or
                >= '0' and <= '9')
            {
                result.Append(c);
                lastWasHyphen = false;
            }
            else if (c is >= 'A' and <= 'Z')
            {
                result.Append(char.ToLowerInvariant(c));
                lastWasHyphen = false;
            }
            else if (lastWasHyphen == false && result.Length > 0) // Add hyphen for any other character
            {
                result.Append('-');
                lastWasHyphen = true;
            }
        }

        // Trim any trailing hyphens
        var finalResult = result.ToString().TrimEnd('-');

        // Ensure we have at least one character
        return string.IsNullOrEmpty(finalResult) ? $"{nameof(AiConnectionString)}Identifier" : finalResult;
    }
}
