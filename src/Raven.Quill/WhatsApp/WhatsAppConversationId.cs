using System.Globalization;

namespace Raven.Quill.WhatsApp;

internal static class WhatsAppConversationId
{
    internal static string For(string channelId, string senderJid, DateTime utcNow) =>
        string.Create(CultureInfo.InvariantCulture,
            $"chats/wa/{channelId}/{SenderDigits(senderJid)}/{utcNow:yyyy-MM-dd}");

    internal static string SenderDigits(string senderJid)
    {
        var user = senderJid;
        var at = user.IndexOf('@');
        if (at >= 0)
            user = user[..at];

        var colon = user.IndexOf(':');
        if (colon >= 0)
            user = user[..colon];

        return user;
    }
}
