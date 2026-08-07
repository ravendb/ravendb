namespace Raven.Quill.Channels;

internal sealed class TelegramSettings
{
    internal const string UserIdentifierParameterName = "UserIdentifier";

    internal const string TelegramUsernameParameterName = "TelegramUsername";

    internal static bool IsAutoBoundParameter(string name) =>
        string.Equals(name, UserIdentifierParameterName, StringComparison.OrdinalIgnoreCase) ||
        string.Equals(name, TelegramUsernameParameterName, StringComparison.OrdinalIgnoreCase);

    internal static string RedactToken(string? token)
    {
        if (string.IsNullOrEmpty(token))
            return "";

        var colon = token.IndexOf(':');
        return colon > 0 ? token[..colon] + ":..." : "...";
    }

    internal static string ScrubToken(string text, string? token) =>
        string.IsNullOrEmpty(token) ? text : text.Replace(token, RedactToken(token));

    public string BotToken { get; set; } = "";

    public long BotId { get; set; }

    public string BotUsername { get; set; } = "";

    public Dictionary<string, string> Parameters { get; set; } = new();
}
