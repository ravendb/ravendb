using System.Globalization;
using Raven.Quill.Channels;

namespace Raven.Quill.Telegram;

internal static class TelegramConversationId
{
    // chats/ prefix satisfies AgentRouter.TryNormalizeConversationId; the UTC date
    // segment rolls the same chat to a fresh conversation at midnight, and the
    // bindings fingerprint rolls it when the channel's parameter bindings change
    internal static string ForUtcDay(
        string channelId, long telegramChatId, DateTime utcNow,
        Dictionary<string, ChannelParameterBinding> parameterBindings) =>
        string.Create(CultureInfo.InvariantCulture,
            $"chats/tg/{channelId}/{telegramChatId}/{utcNow:yyyy-MM-dd}/{ChannelParameterBindings.Fingerprint(parameterBindings)}");
}
