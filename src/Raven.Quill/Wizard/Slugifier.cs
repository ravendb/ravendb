using System.Text;

namespace Raven.Quill.Wizard;

internal static class Slugifier
{
    public const int MaxLength = global::Raven.Client.Constants.Documents.MaxDatabaseNameLength;

    public static bool IsWellFormed(string? slug)
    {
        if (string.IsNullOrEmpty(slug) || slug.Length > MaxLength)
            return false;

        var lastWasDash = true;
        foreach (var ch in slug)
        {
            if (ch is >= 'a' and <= 'z' or >= '0' and <= '9')
                lastWasDash = false;
            else if (ch == '-' && lastWasDash == false)
                lastWasDash = true;
            else
                return false;
        }

        return lastWasDash == false;
    }

    public static string ToSlug(string? appName)
    {
        if (string.IsNullOrWhiteSpace(appName))
            return string.Empty;

        var sb = new StringBuilder(appName.Length);
        var lastWasDash = false;
        foreach (var rune in appName.Trim())
        {
            if (rune is >= 'a' and <= 'z' or >= 'A' and <= 'Z' or >= '0' and <= '9')
            {
                sb.Append(char.ToLowerInvariant(rune));
                lastWasDash = false;
            }
            else if (!lastWasDash && sb.Length > 0)
            {
                sb.Append('-');
                lastWasDash = true;
            }
        }

        if (sb.Length > 0 && sb[^1] == '-')
            sb.Length--;

        return sb.ToString();
    }
}
