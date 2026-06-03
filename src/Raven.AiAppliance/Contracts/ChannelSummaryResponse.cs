using Raven.AiAppliance.Channels;

namespace Raven.AiAppliance.Contracts;

/// <summary>
/// Dashboard-facing channel summary. Deliberately curated: it never projects
/// the channel's binding id, allowed-origins list, or any secret — the
/// "no secrets" rule for <c>GET /api/apps/{slug}/channels</c>.
/// </summary>
/// <param name="WidgetId">The channel id (the part after the <c>channels/</c>
/// doc-prefix). For an iFrame channel this is the public widgetId.</param>
public sealed record ChannelSummaryResponse(
    string WidgetId,
    ChannelType Type,
    string AgentId,
    string DisplayName,
    bool Enabled,
    DateTime CreatedAt)
{
    private const string IdPrefix = "channels/";

    internal static ChannelSummaryResponse From(Channel channel) => new(
        StripPrefix(channel.Id),
        channel.Type,
        channel.AgentId,
        channel.DisplayName,
        channel.Enabled,
        channel.CreatedAt);

    private static string StripPrefix(string? id) =>
        id is not null && id.StartsWith(IdPrefix, StringComparison.Ordinal)
            ? id[IdPrefix.Length..]
            : id ?? "";
}
