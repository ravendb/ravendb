using System.Globalization;
using Raven.Quill.Channels;

namespace Raven.Quill.Telegram;

internal static class TelegramConversationId
{
    internal static string For(
        string channelId, long telegramChatId, Dictionary<string, string> parameters) =>
        ChannelConversationId.For(
            "tg", channelId, telegramChatId.ToString(CultureInfo.InvariantCulture), parameters);

    internal static string ChatPrefix(string channelId, long telegramChatId) =>
        ChannelConversationId.ChatPrefix(
            "tg", channelId, telegramChatId.ToString(CultureInfo.InvariantCulture));
}
