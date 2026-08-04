using System.Globalization;

namespace Raven.Quill.Telegram;

internal static class TelegramConversationId
{
    // chats/ prefix satisfies AgentRouter.TryNormalizeConversationId; the UTC date
    // segment is the daily epoch window, so the same chat rolls to a fresh
    // conversation at midnight (ConversationDurationHours = 24 default)
    internal static string For(string channelId, long telegramChatId, DateTime utcNow) =>
        string.Create(CultureInfo.InvariantCulture,
            $"chats/tg/{channelId}/{telegramChatId}/{utcNow:yyyy-MM-dd}");
}
