using System.Text;

namespace Raven.Quill.Wizard;

internal static class Slugifier
{
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
