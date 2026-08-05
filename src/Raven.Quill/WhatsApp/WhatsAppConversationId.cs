using System.Globalization;

namespace Raven.Quill.WhatsApp;

internal static class WhatsAppConversationId
{
    // chats/ prefix satisfies AgentRouter.TryNormalizeConversationId; the UTC date
    // segment is the daily epoch window, so the same sender rolls to a fresh
    // conversation at midnight (ConversationDurationHours = 24 default)
    internal static string For(string channelId, string senderJid, DateTime utcNow) =>
        string.Create(CultureInfo.InvariantCulture,
            $"chats/wa/{channelId}/{SenderDigits(senderJid)}/{utcNow:yyyy-MM-dd}");

    /// "48123456789:5@s.whatsapp.net" -> "48123456789" (device suffix and domain stripped)
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
