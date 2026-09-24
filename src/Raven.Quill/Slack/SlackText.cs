namespace Raven.Quill.Slack;

internal static class SlackText
{
    internal const int MaxEscapeExpansion = 5;

    internal static string Escape(string text) =>
        string.IsNullOrEmpty(text)
            ? text
            : text.Replace("&", "&amp;").Replace("<", "&lt;").Replace(">", "&gt;");
}
