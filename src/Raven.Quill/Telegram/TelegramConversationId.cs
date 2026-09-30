using System.Globalization;

namespace Raven.Quill.Telegram;

internal static class TelegramConversationId
{
    // chats/ prefix satisfies AgentRouter.TryNormalizeConversationId; the UTC date
    // segment rolls the same chat to a fresh conversation at midnight
    internal static string ForUtcDay(string channelId, long telegramChatId, DateTime utcNow) =>
        string.Create(CultureInfo.InvariantCulture,
            $"chats/tg/{channelId}/{telegramChatId}/{utcNow:yyyy-MM-dd}");
}
