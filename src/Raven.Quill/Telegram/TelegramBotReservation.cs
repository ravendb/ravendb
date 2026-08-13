namespace Raven.Quill.Telegram;

// Lives in the config-store database so one bot token can never poll from two apps.
internal sealed class TelegramBotReservation
{
    internal const string IdPrefix = "telegram-bots/";

    internal static string IdFor(long botId) => $"{IdPrefix}{botId}";

    public string? Id { get; set; }

    public string Database { get; set; } = "";

    public string ChannelId { get; set; } = "";
}
