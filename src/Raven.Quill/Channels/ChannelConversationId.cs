using System.Globalization;
using System.Security.Cryptography;
using System.Text;

namespace Raven.Quill.Channels;

internal static class ChannelConversationId
{
    internal static string ForUtcDay(
        string kind, string channelId, string userId, DateTime utcNow,
        Dictionary<string, string> parameters) =>
        string.Create(CultureInfo.InvariantCulture,
            $"chats/{kind}/{channelId}/{userId}/{utcNow:yyyy-MM-dd}/{Fingerprint(parameters)}");

    internal static string Fingerprint(Dictionary<string, string> parameters)
    {
        var canonical = new StringBuilder();
        foreach (var (name, value) in parameters.OrderBy(entry => entry.Key, StringComparer.OrdinalIgnoreCase))
        {
            canonical.Append(name.ToLowerInvariant()).Append('\u001f')
                .Append(value).Append('\u001e');
        }

        var hash = SHA256.HashData(Encoding.UTF8.GetBytes(canonical.ToString()));
        return Convert.ToHexString(hash.AsSpan(0, 4)).ToLowerInvariant();
    }
}
