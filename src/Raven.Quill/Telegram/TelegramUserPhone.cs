namespace Raven.Quill.Telegram;

/// Telegram delivers the phone number once, on the contact-share message, so it is persisted per sender.
internal sealed class TelegramUserPhone
{
    internal static string IdFor(string channelId, long userId) =>
        $"telegram/phones/{channelId}/{userId}";

    public string? Id { get; set; }

    public string PhoneNumber { get; set; } = "";

    public DateTime SharedAt { get; set; }
}
