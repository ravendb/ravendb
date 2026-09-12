namespace Raven.Quill.Telegram;

internal sealed class TelegramLink
{
    internal const string IdPrefix = "telegram-links/";

    internal static string IdFor(string channelId, long userId) => $"{IdPrefix}{channelId}/{userId}";

    public string? Id { get; set; }

    public string PhoneNumber { get; set; } = "";

    public DateTime SharedAt { get; set; }
}
