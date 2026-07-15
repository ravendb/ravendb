using Raven.Quill.Channels;

namespace Raven.Quill.Contracts;

// no secrets: never projects binding id / allowed-origins
public sealed record ChannelSummaryResponse(
    string WidgetId,
    ChannelType Type,
    string AgentId,
    string DisplayName,
    bool Enabled,
    DateTime CreatedAt)
{
    internal static ChannelSummaryResponse From(Channel channel) => new(
        StripPrefix(channel.Id),
        channel.Type,
        channel.AgentId,
        channel.DisplayName,
        channel.Enabled,
        channel.CreatedAt);

    private static string StripPrefix(string? id) =>
        id is not null && id.StartsWith(Channel.IdPrefix, StringComparison.Ordinal)
            ? id[Channel.IdPrefix.Length..]
            : id ?? "";
}
