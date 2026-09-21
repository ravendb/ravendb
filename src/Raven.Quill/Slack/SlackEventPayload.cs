using System.Text.Json;
using System.Text.Json.Serialization;

namespace Raven.Quill.Slack;

internal sealed class SlackSocketFrame
{
    internal const string HelloType = "hello";
    internal const string DisconnectType = "disconnect";
    internal const string EventsApiType = "events_api";

    [JsonPropertyName("type")]
    public string? Type { get; set; }

    [JsonPropertyName("envelope_id")]
    public string? EnvelopeId { get; set; }

    [JsonPropertyName("reason")]
    public string? Reason { get; set; }

    [JsonPropertyName("retry_attempt")]
    public int? RetryAttempt { get; set; }

    [JsonPropertyName("payload")]
    public JsonElement? Payload { get; set; }
}

internal sealed class SlackEventPayload
{
    [JsonPropertyName("type")]
    public string? Type { get; set; }

    [JsonPropertyName("team_id")]
    public string? TeamId { get; set; }

    [JsonPropertyName("event_id")]
    public string? EventId { get; set; }

    [JsonPropertyName("event")]
    public SlackEvent? Event { get; set; }
}

internal sealed class SlackEvent
{
    [JsonPropertyName("type")]
    public string? Type { get; set; }

    [JsonPropertyName("subtype")]
    public string? Subtype { get; set; }

    [JsonPropertyName("channel")]
    public string? Channel { get; set; }

    [JsonPropertyName("channel_type")]
    public string? ChannelType { get; set; }

    [JsonPropertyName("user")]
    public string? User { get; set; }

    [JsonPropertyName("bot_id")]
    public string? BotId { get; set; }

    [JsonPropertyName("text")]
    public string? Text { get; set; }
}
