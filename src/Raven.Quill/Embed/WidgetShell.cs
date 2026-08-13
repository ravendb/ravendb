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

        builder.Append($"<style nonce=\"{nonce}\">{BuildRootBlock(theme)}</style>\n");
        builder.Append("</head>\n<body>\n<div id=\"rq-root\"></div>\n");

        // A JSON block cannot execute, which keeps the bearer token out of any global and out of reach of
        // anything that later manages to run in this document.
        builder.Append($"<script type=\"application/json\" id=\"rq-config\" nonce=\"{nonce}\">{configJson}</script>\n");
        builder.Append($"<script type=\"module\" src=\"{assets.ScriptUrl}\" nonce=\"{nonce}\"></script>\n");
        builder.Append("</body>\n</html>\n");

        return builder.ToString();
    }

    /// Only the operator's raw inputs, never a derived colour: the derivation lives in the widget's
    /// TypeScript, and duplicating it here is exactly the drift this rewrite set out to remove. These are
    /// enough for the browser to paint the right background before the bundle mounts.
    internal static string BuildRootBlock(WidgetTheme theme)
    {
        // Defence for a document written outside the PUT path: an unvalidated theme is discarded whole
        // rather than field by field, so nothing half-trusted reaches the stylesheet.
        var safe = WidgetThemeValidation.TryValidate(theme, out _) ? theme : WidgetTheme.Default;
        var radius = Math.Clamp(safe.Radius, 0, WidgetThemeValidation.MaxRadius);

        return $":root{{color-scheme:{ColorScheme(safe.Appearance)};--rq-accent:{safe.AccentColor};" +
               $"--rq-radius:{radius}px;--rq-font:{safe.FontFamily}}}";
    }

    private static string ColorScheme(WidgetAppearance appearance) => appearance switch
    {
        WidgetAppearance.Light => "light",
        WidgetAppearance.Dark => "dark",
        _ => "light dark",
    };

    /// `JavaScriptEncoder.Default` escapes `<`, `>` and `&`, so a message body containing `</script>` can
    /// never close the config block. The app's converters are kept so enums stay camel-cased strings.
    public static string SerializeConfig(object config, JsonSerializerOptions appOptions) =>
        JsonSerializer.Serialize(config, new JsonSerializerOptions(appOptions) { Encoder = JavaScriptEncoder.Default });
}
