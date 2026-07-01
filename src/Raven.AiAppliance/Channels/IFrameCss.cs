namespace Raven.AiAppliance.Channels;

/// <summary>
/// Validation + render-time hardening for operator-authored web-widget (iFrame) embed CSS,
/// shared by the iFrame customization PUT endpoints (validate on save) and the embed page
/// renderer (defensive strip on render). The CSS is injected into a <c>&lt;style&gt;</c>
/// element server-side, so a literal <c>&lt;/style&gt;</c> would break out of the element into
/// HTML — the one sequence that turns operator CSS into stored XSS.
/// </summary>
internal static class IFrameCss
{
    /// <summary>Caps stored CSS so a channel/defaults doc can't grow unbounded. Generous next
    /// to any hand-written theme (tens of KB at most).</summary>
    internal const int MaxLength = 100_000;

    // The only sequence that ends a <style> element's raw-text content. HTML requires the "</"
    // to be contiguous and the tag name to follow immediately, so a case-insensitive substring
    // check is exact — no regex (and no source-generated partial) needed.
    private const string StyleClose = "</style";

    /// <summary>Validates CSS accepted on a PUT. Null/empty is valid (clears to the default).
    /// Rejects over-length input and any <c>&lt;/style&gt;</c> breakout sequence.</summary>
    internal static bool TryValidate(string? css, out string? error)
    {
        error = null;
        if (string.IsNullOrEmpty(css))
            return true;

        if (css.Length > MaxLength)
        {
            error = $"css exceeds {MaxLength} chars";
            return false;
        }

        if (css.Contains(StyleClose, StringComparison.OrdinalIgnoreCase))
        {
            error = "css must not contain a '</style>' sequence";
            return false;
        }

        return true;
    }

    /// <summary>Defense-in-depth for render: neutralizes any <c>&lt;/style&gt;</c> breakout so a
    /// doc written outside the PUT path can't escape the <c>&lt;style&gt;</c> element. Inserting
    /// the backslash stops the HTML tokenizer from seeing an end tag without altering how the
    /// (already invalid) CSS renders.</summary>
    internal static string Sanitize(string? css) =>
        string.IsNullOrEmpty(css) ? "" : css.Replace(StyleClose, @"<\/style", StringComparison.OrdinalIgnoreCase);
}
