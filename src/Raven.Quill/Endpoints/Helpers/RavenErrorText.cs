using System.Text.RegularExpressions;

namespace Raven.Quill.Endpoints.Helpers;

internal static partial class RavenErrorText
{
    private const int MaxLength = 1024;

    [GeneratedRegex(@"[A-Za-z0-9_.]+[.+](\w+(?:Exception|Error)): ")]
    private static partial Regex TypePrefix();

    public static string Reason(Exception exception)
    {
        var lines = exception.Message
            .Split('\n')
            .Select(static line => line.TrimEnd('\r'))
            .TakeWhile(static line => line.StartsWith("   at ", StringComparison.Ordinal) == false)
            .Where(static line =>
                string.IsNullOrWhiteSpace(line) == false &&
                line.StartsWith("The server at ", StringComparison.Ordinal) == false);

        var message = TypePrefix().Replace(string.Join(" ", lines), "$1: ").Trim();
        return message.Length > MaxLength ? message[..MaxLength] + "..." : message;
    }
}
