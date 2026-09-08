using System.Text.Json;
using System.Text.Json.Serialization;

namespace Raven.Quill.Discord;

internal static class DiscordGatewayOpcode
{
    internal const int Dispatch = 0;
    internal const int Heartbeat = 1;
    internal const int Identify = 2;
    internal const int Resume = 6;
    internal const int Reconnect = 7;
    internal const int InvalidSession = 9;
    internal const int Hello = 10;
    internal const int HeartbeatAck = 11;
}

internal sealed class DiscordGatewayFrame
{
    [JsonPropertyName("op")]
    public int Op { get; set; }

    [JsonPropertyName("d")]
    public JsonElement? D { get; set; }

    [JsonPropertyName("s")]
    public long? S { get; set; }

    [JsonPropertyName("t")]
    public string? T { get; set; }
}

internal sealed class DiscordHelloPayload
{
    [JsonPropertyName("heartbeat_interval")]
    public int HeartbeatInterval { get; set; }
}

internal sealed class DiscordReadyPayload
{
    [JsonPropertyName("session_id")]
    public string? SessionId { get; set; }

    [JsonPropertyName("resume_gateway_url")]
    public string? ResumeGatewayUrl { get; set; }
}

internal sealed class DiscordMessagePayload
{
    [JsonPropertyName("id")]
    public string? Id { get; set; }

    [JsonPropertyName("channel_id")]
    public string? ChannelId { get; set; }

    [JsonPropertyName("guild_id")]
    public string? GuildId { get; set; }

    [JsonPropertyName("type")]
    public int Type { get; set; }

    [JsonPropertyName("content")]
    public string? Content { get; set; }

    [JsonPropertyName("author")]
    public DiscordAuthorPayload? Author { get; set; }

    [JsonPropertyName("attachments")]
    public DiscordAttachmentPayload[]? Attachments { get; set; }
}

internal sealed class DiscordAttachmentPayload
{
    [JsonPropertyName("id")]
    public string? Id { get; set; }
}

internal sealed class DiscordAuthorPayload
{
    [JsonPropertyName("id")]
    public string? Id { get; set; }

    [JsonPropertyName("username")]
    public string? Username { get; set; }

    [JsonPropertyName("bot")]
    public bool? Bot { get; set; }
}
