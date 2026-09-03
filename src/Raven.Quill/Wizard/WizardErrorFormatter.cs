using System.Text.RegularExpressions;

namespace Raven.Quill.Wizard;

internal static partial class WizardErrorFormatter
{
    private const string ConnectHint =
        "Could not connect to the source database. Check that the host and port are reachable, the " +
        "database name is correct, and the credentials are valid.";

    private const string UnknownError = "Unknown error.";

    // Matches the "Some.Namespace.SomeException: " (optionally "(0x80004005)") prefix that leads
    // ex.ToString()/ex.Message and any nested "---> Inner.Exception: " markers.
    [GeneratedRegex(@"[\w.+`]*Exception(?:\s*\(0x[0-9A-Fa-f]+\))?:\s*", RegexOptions.CultureInvariant)]
    private static partial Regex ExceptionPrefix();

    // Keeps the human-readable part of a raw error: first line only (drops the stack trace), with
    // exception type prefixes and inner-exception markers removed.
    public static string Sanitize(string? rawError)
    {
        if (string.IsNullOrWhiteSpace(rawError))
            return UnknownError;

        var firstLine = rawError.Split('\n', 2)[0].Replace("\r", string.Empty);
        firstLine = ExceptionPrefix().Replace(firstLine, string.Empty);
        firstLine = firstLine.Replace(" ---> ", " ").Trim();

        return firstLine.Length == 0 ? UnknownError : firstLine;
    }

    // Splits a raw error into a one-line summary and the full raw text (stack trace included),
    // which is kept in Details. Details is null when the raw text adds nothing beyond the summary.
    public static WizardError Format(string? rawError)
    {
        var summary = Sanitize(rawError);
        var full = rawError?.Trim();
        var details = string.IsNullOrEmpty(full) || full == summary ? null : full;
        return new WizardError(summary, details);
    }

    // Same as Format, but prepends the actionable connectivity hint to the summary.
    public static WizardError FormatConnectionError(string? rawError)
    {
        var error = Format(rawError);
        error.Message = error.Message == UnknownError ? ConnectHint : $"{ConnectHint} {error.Message}";
        return error;
    }
}
