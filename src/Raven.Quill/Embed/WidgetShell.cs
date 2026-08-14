using System.Net;
using System.Security.Cryptography;
using System.Text;
using System.Text.Encodings.Web;
using System.Text.Json;
using Raven.Quill.Channels;

namespace Raven.Quill.Embed;

/// Builds the thin HTML shell that boots the widget bundle. Everything the widget needs arrives either as
/// a CSS custom property (so the first paint is already themed) or in the JSON config block.
public static class WidgetShell
{
    private const int NonceBytes = 16;

    public static string CreateNonce() => Convert.ToBase64String(RandomNumberGenerator.GetBytes(NonceBytes));

    /// No `'unsafe-inline'` in either directive: the one inline style and the one inline JSON block both
    /// carry the request's nonce, and every script the browser executes comes from `/widget/`.
    /// An empty <paramref name="frameAncestors"/> omits the directive entirely, which is the channel's
    /// explicit "embeddable from anywhere" contract rather than an accident.
    public static string BuildCsp(string nonce, IEnumerable<string> frameAncestors)
    {
        string[] directives =
        [
            "default-src 'none'",
            $"script-src 'self' 'nonce-{nonce}'",
            $"style-src 'self' 'nonce-{nonce}'",
            "img-src 'self' data:",
            "font-src 'self'",
            "connect-src 'self'",
            "base-uri 'none'",
            "form-action 'none'",
        ];

        var ancestors = string.Join(' ', frameAncestors);
        return ancestors.Length == 0
            ? string.Join("; ", directives)
            : $"{string.Join("; ", directives)}; frame-ancestors {ancestors}";
    }

    public static string BuildHtml(WidgetAssets assets, string nonce, string title, WidgetTheme theme, string configJson)
    {
        // Defence for a document written outside the PUT path: an unvalidated theme is discarded whole
        // rather than field by field, so nothing half-trusted reaches a stylesheet.
        var safe = WidgetThemeValidation.TryValidate(theme, out _) ? theme : WidgetTheme.Default;

        var builder = new StringBuilder();
        builder.Append("<!doctype html>\n<html lang=\"en\">\n<head>\n");
        builder.Append("<meta charset=\"utf-8\">\n");
        builder.Append("<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">\n");
        builder.Append("<meta name=\"referrer\" content=\"no-referrer\">\n");
        builder.Append($"<title>{WebUtility.HtmlEncode(title)}</title>\n");

        foreach (var style in assets.StyleUrls)
            builder.Append($"<link rel=\"stylesheet\" href=\"{style}\">\n");

        foreach (var module in assets.ModuleUrls)
            builder.Append($"<link rel=\"modulepreload\" href=\"{module}\">\n");

        builder.Append($"<style nonce=\"{nonce}\">{BuildRootBlock(safe)}</style>\n");

        // Last in the head, so the operator's rules win the cascade over the widget's own stylesheets.
        // Validation guarantees the text cannot contain `</style`, so it cannot close this tag.
        if (string.IsNullOrEmpty(safe.CustomCss) == false)
            builder.Append($"<style nonce=\"{nonce}\">{safe.CustomCss}</style>\n");

        builder.Append("</head>\n<body>\n<div id=\"rq-root\"></div>\n");

        // A JSON block cannot execute, which keeps the bearer token out of any global and out of reach of
        // anything that later manages to run in this document.
        builder.Append($"<script type=\"application/json\" id=\"rq-config\" nonce=\"{nonce}\">{configJson}</script>\n");
        builder.Append($"<script type=\"module\" src=\"{assets.ScriptUrl}\" nonce=\"{nonce}\"></script>\n");
        builder.Append("</body>\n</html>\n");

        return builder.ToString();
    }

    /// Only the operator's raw inputs, never a derived color: the derivation lives in the widget's
    /// TypeScript, and duplicating it here is exactly the drift this rewrite set out to remove. These are
    /// enough for the browser to paint the right background before the bundle mounts. Both schemes' values
    /// are emitted through light-dark(), so the first paint is right whichever way `color-scheme` resolves.
    internal static string BuildRootBlock(WidgetTheme theme)
    {
        // Same discard-whole defence as BuildHtml, kept here too because the notice pages call this alone.
        var safe = WidgetThemeValidation.TryValidate(theme, out _) ? theme : WidgetTheme.Default;

        return $":root{{color-scheme:{ColorScheme(safe.Appearance)};" +
               $"--rq-accent:light-dark({safe.Light.ButtonColor},{safe.Dark.ButtonColor});" +
               $"--rq-bg:light-dark({safe.Light.BackgroundColor},{safe.Dark.BackgroundColor});" +
               $"--rq-radius:{RadiusPx(safe.Radius)}px;--rq-font:{safe.FontFamily};" +
               $"font-size:{FontSizeRem(safe)}rem}}";
    }

    private static string ColorScheme(WidgetAppearance appearance) => appearance switch
    {
        WidgetAppearance.Light => "light",
        WidgetAppearance.Dark => "dark",
        _ => "light dark",
    };

    // Kept in step with RADIUS_SCALE in packages/widget/src/widget-theme.ts; only the base value is needed
    // here, because the first paint has no corners smaller than the surface radius to round.
    private static int RadiusPx(WidgetRadius radius) => radius switch
    {
        WidgetRadius.None => 0,
        WidgetRadius.Small => 6,
        WidgetRadius.Large => 18,
        _ => 12,
    };

    // Kept in step with FONT_SIZE_REM / resolveFontSizeRem in packages/widget/src/widget-theme.ts. Applied
    // to :root, so every rem-based size inside the document scales with it.
    private static string FontSizeRem(WidgetTheme theme)
    {
        var rem = theme.FontSize switch
        {
            WidgetFontSize.Small => 0.875,
            WidgetFontSize.Large => 1.125,
            WidgetFontSize.Custom => Math.Clamp(
                theme.CustomFontSizeRem ?? 1,
                WidgetThemeValidation.MinCustomFontSizeRem,
                WidgetThemeValidation.MaxCustomFontSizeRem),
            _ => 1,
        };

        return rem.ToString("0.####", System.Globalization.CultureInfo.InvariantCulture);
    }

    /// `JavaScriptEncoder.Default` escapes `<`, `>` and `&`, so a message body containing `</script>` can
    /// never close the config block. The app's converters are kept so enums stay camel-cased strings.
    public static string SerializeConfig(object config, JsonSerializerOptions appOptions) =>
        JsonSerializer.Serialize(config, new JsonSerializerOptions(appOptions) { Encoder = JavaScriptEncoder.Default });
}
