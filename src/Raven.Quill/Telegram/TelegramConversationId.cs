using System.Globalization;
using Raven.Quill.Channels;

namespace Raven.Quill.Telegram;

internal static class TelegramConversationId
{
    internal static string ForUtcDay(
        string channelId, long telegramChatId, DateTime utcNow, Dictionary<string, string> parameters) =>
        ChannelConversationId.ForUtcDay(
            "tg", channelId, telegramChatId.ToString(CultureInfo.InvariantCulture), utcNow, parameters);
}
