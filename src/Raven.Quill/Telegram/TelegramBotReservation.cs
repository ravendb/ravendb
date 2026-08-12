namespace Raven.Quill.Telegram;

/// One doc per connected bot; storing it with an empty change vector makes the
/// bot-to-channel assignment unique per app even under concurrent requests.
internal sealed class TelegramBotReservation
{
    internal const string IdPrefix = "telegram-bots/";

    internal static string IdFor(long botId) => $"{IdPrefix}{botId}";

    public string? Id { get; set; }

    public string ChannelId { get; set; } = "";
}
