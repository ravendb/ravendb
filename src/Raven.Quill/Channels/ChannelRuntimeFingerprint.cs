using System.Security.Cryptography;
using System.Text.Json;

namespace Raven.Quill.Channels;

internal static class ChannelRuntimeFingerprint
{
    private static readonly JsonSerializerOptions Options = new() { DefaultIgnoreCondition = System.Text.Json.Serialization.JsonIgnoreCondition.WhenWritingNull };

    public static string Of(Channel channel)
    {
        var inputs = JsonSerializer.SerializeToUtf8Bytes(
            new { channel.Type, channel.AgentId, channel.Enabled, channel.Telegram, channel.Slack, channel.Discord },
            Options);
        return Convert.ToHexStringLower(SHA256.HashData(inputs));
    }
}
