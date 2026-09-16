using System.Text.Json;

namespace Raven.Quill.Contracts;

public sealed record MintEmbedLinkRequest(
    string ChannelId,
    Dictionary<string, JsonElement>? Parameters = null,
    int? TtlSeconds = null,
    int? MaxInvocations = null);

public static class EmbedLinkLimits
{
    public const int MinTtlSeconds = 60; // 1 minute
    public const int MaxTtlSeconds = 30 * 24 * 60 * 60; // 30 days
    public const int DefaultTtlSeconds = 60 * 60; // 1 hour

    public const int MaxMaxInvocations = 1_000_000;
    public const int DefaultMaxInvocations = 100;
}
