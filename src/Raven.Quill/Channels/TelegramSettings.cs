namespace Raven.Quill.Channels;

// bot token stored raw (app DB encrypted at rest), same discipline as EmbedLink:
// never projected in DTOs, redacted in logs and health state
internal sealed class TelegramSettings
{
    /// The agent-declared parameter (matched case-insensitively) that receives the sender's Telegram user id
    /// per message; every other declared parameter must be operator-bound at provision time.
    internal const string UserIdentifierParameterName = "UserIdentifier";

    // log the bot-id part only ("12345678:..."), the rest is the secret
    internal static string RedactToken(string? token)
    {
        if (string.IsNullOrEmpty(token))
            return "";

        var colon = token.IndexOf(':');
        return colon > 0 ? token[..colon] + ":..." : "...";
    }

    // scrub a token out of free text (exception messages embed /bot{token}/ urls)
    internal static string ScrubToken(string text, string? token) =>
        string.IsNullOrEmpty(token) ? text : text.Replace(token, RedactToken(token));

    public string BotToken { get; set; } = "";

    public long BotId { get; set; }

    public string BotUsername { get; set; } = "";

    // operator-bound values for the agent's declared parameters (mint-time-binding analogue)
    public Dictionary<string, string> Parameters { get; set; } = new();
}
