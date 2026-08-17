using System.Net;
using Raven.Quill.Channels;

namespace Raven.Quill.Embed;

/// A styled standalone page for every embed state that has no widget to show. Without it, an expired
/// link — the *normal* end state of every embed, since the default TTL is an hour — answers 410 with an
/// empty body, which a visitor sees as a blank rectangle and an operator sees as nothing at all.
///
/// It carries no bundle on purpose: it has to render when the widget assets are missing, and it has to
/// stay a single self-contained response.
internal static class WidgetNotice
{
    internal readonly record struct Notice(string Type, string Heading, string Body, object Payload)
    {
        internal static Notice Expired(string reason) => new(
            "expired",
            "This conversation has ended",
            "The link that opened it has expired or was revoked. Reload the page to start a new one.",
            new { reason });

        internal static Notice NotFound() => new(
            "error",
            "This conversation is not available",
            "The link is unknown. Reload the page to start a new one.",
            new { message = "not found" });

        internal static Notice Unavailable() => new(
            "error",
            "The assistant is unavailable",
            "It could not be loaded on this server. Please try again later.",
            new { message = "widget unavailable" });
    }

    /// Notice pages never emit `frame-ancestors`: they hold no conversation data, no token-derived content
    /// and no controls, so there is nothing for a hostile frame to gain by displaying them — and every
    /// framing host should see the notice (and receive its postMessage) instead of a blanked-out document.
    internal static string BuildCsp(string nonce) => WidgetShell.BuildCsp(nonce, []);

    /// <paramref name="theme"/> comes from <see cref="WidgetThemeResolution"/>, which is where an untrusted
    /// document is discarded.
    internal static string BuildHtml(WidgetTheme theme, string nonce, Notice notice)
    {
        var script = HostChannel.BuildPostMessageScript(notice.Type, notice.Payload);

        return $"""
            <!doctype html>
            <html lang="en">
            <head>
            <meta charset="utf-8">
            <meta name="viewport" content="width=device-width, initial-scale=1">
            <meta name="referrer" content="no-referrer">
            <title>{WebUtility.HtmlEncode(theme.HeaderTitle)}</title>
            <style nonce="{nonce}">{BuildStyles(theme)}</style>
            </head>
            <body>
            <div class="rq-notice" role="status">
            <h1>{WebUtility.HtmlEncode(notice.Heading)}</h1>
            <p>{WebUtility.HtmlEncode(notice.Body)}</p>
            </div>
            <script nonce="{nonce}">{script}</script>
            </body>
            </html>

            """;
    }

    // The four neutrals the widget paints with, from NEUTRALS in packages/widget/src/widget-theme.ts.
    // Duplicated rather than derived: nothing here mixes or contrasts colors, and a standalone page
    // cannot ask the bundle. light-dark() is declared after a plain light value, so a browser that does
    // not know the function keeps the light palette instead of losing the declaration.
    private static string BuildStyles(WidgetTheme theme) =>
        $"{WidgetShell.BuildRootBlock(theme)}" +
        "html,body{height:100%;margin:0}" +
        "body{display:flex;align-items:center;justify-content:center;padding:24px;" +
        "font-family:var(--rq-font);-webkit-font-smoothing:antialiased;" +
        "background:#ffffff;color:#101828;" +
        "background:light-dark(#ffffff,#0d1117);color:light-dark(#101828,#e6e9ef)}" +
        ".rq-notice{max-width:22rem;text-align:center}" +
        "h1{margin:0 0 .5rem;font-size:.9375rem;font-weight:600;letter-spacing:-.01em}" +
        "p{margin:0;font-size:.8125rem;line-height:1.55;color:#596273;color:light-dark(#596273,#98a2b3)}";
}
