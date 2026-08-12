namespace Raven.Quill.Telegram;

internal sealed class TelegramBotReservation
{
    internal const string IdPrefix = "telegram-bots/";

    internal static string IdFor(long botId) => $"{IdPrefix}{botId}";

    public string? Id { get; set; }

    public string ChannelId { get; set; } = "";
}
