using System.Globalization;
using System.Security.Cryptography;
using System.Text;

namespace Raven.Quill.Channels;

internal static class ChannelConversationId
{
    // chats/ prefix satisfies AgentRouter.TryNormalizeConversationId; the id is stable per chat,
    // rolling only via the fingerprint when a resolved parameter value changes, since the server
    // binds parameters only at creation; lifetime is governed by the conversation's idle TTL
    internal static string For(
        ChannelType type, string channelId, string userId,
        Dictionary<string, string> parameters) =>
        ChatPrefix(type, channelId, userId) + Fingerprint(parameters);

    internal static string ChatPrefix(ChannelType type, string channelId, string userId) =>
        string.Create(CultureInfo.InvariantCulture,
            $"chats/{Segment(type)}/{channelId}/{userId}/");

    // persisted id segments: pinned literals, never derived from member names, so an enum
    // rename can't silently change ids already stored
    private static string Segment(ChannelType type) => type switch
    {
        ChannelType.Telegram => "telegram",
        ChannelType.Slack => "slack",
        ChannelType.Discord => "discord",
        _ => throw new ArgumentOutOfRangeException(nameof(type), type, "channel type does not derive conversation ids"),
    };

    internal static string Fingerprint(Dictionary<string, string> parameters)
    {
        var canonical = new StringBuilder();
        foreach (var (name, value) in parameters.OrderBy(entry => entry.Key, StringComparer.OrdinalIgnoreCase))
        {
            canonical.Append(name.ToLowerInvariant()).Append('\u001f')
                .Append(value).Append('\u001e');
        }

        var hash = SHA256.HashData(Encoding.UTF8.GetBytes(canonical.ToString()));
        return Convert.ToHexStringLower(hash.AsSpan(0, 8));
    }
}
