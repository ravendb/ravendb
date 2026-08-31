using System.Globalization;
using System.Security.Cryptography;
using System.Text;

namespace Raven.Quill.Channels;

internal static class ChannelConversationId
{
    internal static string ForUtcDay(
        string kind, string channelId, string userId, DateTime utcNow,
        Dictionary<string, string> parameters) =>
        UtcDayPrefix(kind, channelId, userId, utcNow) + Fingerprint(parameters);

    internal static string UtcDayPrefix(string kind, string channelId, string userId, DateTime utcNow) =>
        string.Create(CultureInfo.InvariantCulture,
            $"chats/{kind}/{channelId}/{userId}/{utcNow:yyyy-MM-dd}/");

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
