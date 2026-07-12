using System.Text;

namespace Raven.AiAppliance.Wizard;

/// <summary>
/// Derives an URL-safe, RavenDB-DB-name-safe slug from a user-typed appName.
///
/// Matches the Studio's client-side slug derivation:
/// lowercase, whitespace + any non-alphanumeric character collapsed to a single
/// dash, leading/trailing dashes stripped. Examples:
///
///   "Northwind Demo"    -> "northwind-demo"
///   "Acme Shop!! 2"     -> "acme-shop-2"
///   "  spaces   ok  "   -> "spaces-ok"
///   "----"              -> "" (empty -> caller's job to 400)
///
/// The slug becomes both the per-app RavenDB database name and the URL segment
/// in /api/apps/{slug}/..., so it must satisfy RavenDB's database-name rules
/// (alphanumeric, '-', '_', '.'). Dashes only is the safest subset.
/// </summary>
internal static class Slugifier
{
    /// <summary>
    /// Normalize <paramref name="appName"/> to a slug; returns empty string when
    /// the input has no alphanumeric characters. Callers should treat empty as
    /// a validation failure.
    /// </summary>
    public static string ToSlug(string? appName)
    {
        if (string.IsNullOrWhiteSpace(appName))
            return string.Empty;

        var sb = new StringBuilder(appName.Length);
        var lastWasDash = false;
        foreach (var rune in appName.Trim())
        {
            // ASCII-only: anything outside [A-Za-z0-9] -- including non-ASCII
            // letters (e.g. "é", CJK), punctuation, whitespace -- is treated
            // as a separator and may emit a single dash between two retained
            // ASCII runs ("naïve" -> "na-ve", not "nave"). Sequential
            // separators collapse to one dash; leading/trailing dashes are
            // stripped. Keeps URL segments and RavenDB database names free of
            // any character that needs URL-encoding or shell-quoting. If true
            // transliteration is wanted later, swap this for ICU.
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

        // Strip the single trailing dash (if any). Leading dashes can't be
        // emitted by the loop above because we gate on `sb.Length > 0`.
        if (sb.Length > 0 && sb[^1] == '-')
            sb.Length--;

        return sb.ToString();
    }
}
