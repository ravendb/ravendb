namespace Raven.Quill.Contracts;

// no secrets: the bot token never appears here
public sealed record TelegramChannelHealthResponse(
    string ChannelId,
    string? BotUsername,
    bool Enabled,
    bool IsPolling,
    DateTime? LastSuccessfulPoll,
    DateTime? LastErrorAt,
    int ErrorCount,
    string? LastError);
