using System.Text.RegularExpressions;

namespace Tests.Infrastructure.Utils;

public static class PemUtils
{
    // Regular expression to match and capture the base64 payload between
    // any standard BEGIN/END headers (e.g., RSA PRIVATE KEY, PRIVATE KEY, CERTIFICATE).
    // It captures everything between the first '-----' line and the last '-----' line.
    private static readonly Regex PemPayloadRegex = new Regex(
        @"-----BEGIN [A-Z0-9 ]+-----[\r\n]*(.*?)[\r\n]*-----END [A-Z0-9 ]+-----",
        RegexOptions.Singleline | RegexOptions.Compiled);

    /// <summary>
    /// Normalizes a PEM string by removing all PEM headers, footers,
    /// whitespace, and converting line endings to a single common format (LF).
    /// </summary>
    /// <param name="pemContent">The PEM formatted string (e.g., key or certificate).</param>
    /// <returns>A single string containing only the raw, contiguous Base64 payload,
    /// with all whitespace and line endings removed.</returns>
    public static string NormalizePemContent(string pemContent)
    {
        if (string.IsNullOrEmpty(pemContent))
        {
            return string.Empty;
        }

        // 1. Extract the payload using the regular expression
        var match = PemPayloadRegex.Match(pemContent);

        if (match.Success == false || match.Groups.Count < 2)
        {
            // If it doesn't match the expected PEM structure, return the original
            // content stripped of just common whitespace, just in case it's already raw Base64.
            return StripAllWhitespace(pemContent);
        }

        // The captured group 1 is the Base64 content between headers/footers
        string base64Payload = match.Groups[1].Value;

        // 2. Remove all whitespace (spaces, tabs, and all line endings like CRLF, LF)
        return StripAllWhitespace(base64Payload);
    }

    /// <summary>
    /// Strips all whitespace characters (spaces, tabs, newlines, carriage returns)
    /// from a string.
    /// </summary>
    private static string StripAllWhitespace(string content)
    {
        // Replace all whitespace characters (including \r, \n, \t, space) with an empty string
        return Regex.Replace(content, @"\s", string.Empty);
    }
}
