namespace Raven.Quill.Contracts;

/// <summary>
/// App Overview snapshot: index-free health/usage counts read straight from
/// database statistics and the per-app collections. Conversation activity over
/// time lives on the dedicated <c>conversations/stats</c> endpoint; per-agent
/// usage is folded into the enriched <c>agents</c> list.
/// </summary>
/// <param name="Slug">The app slug.</param>
/// <param name="Documents">Total documents in the app database (data-volume indicator).</param>
/// <param name="ConfiguredAgents">Agents provisioned in the app's RavenDB AI agent registry.</param>
/// <param name="Channels">Total channels provisioned.</param>
/// <param name="ActiveChannels">Channels that are enabled.</param>
public sealed record AppOverviewResponse(
    string Slug,
    long Documents,
    int ConfiguredAgents,
    int Channels,
    int ActiveChannels);
