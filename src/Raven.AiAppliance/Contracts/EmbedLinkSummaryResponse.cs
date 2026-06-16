using Raven.AiAppliance.Channels;

namespace Raven.AiAppliance.Contracts;

/// <summary>
/// Dashboard-facing summary of an active (non-expired, non-revoked) embed link,
/// returned by <c>GET /api/apps/{slug}/embed-links</c> so the operator can see
/// and revoke the links minted against a channel.
/// </summary>
/// <param name="Token">The opaque bearer token (the part after the
/// <c>embed-links/</c> doc-prefix). Pass it to
/// <c>DELETE /api/apps/{slug}/embed-links/{token}</c> to revoke.</param>
/// <param name="WidgetId">The channel this link targets — lets the dashboard
/// group links under their channel.</param>
/// <param name="AgentId">The agent the link routes to.</param>
/// <param name="Parameters">The agent parameters bound at mint time (e.g.
/// <c>{ "Customer": "users/1" }</c>) — the audit trail of who a link is for.</param>
/// <param name="CreatedAt">UTC mint timestamp.</param>
/// <param name="ExpiresAt">UTC instant the link dies (TTL).</param>
/// <param name="MaxInvocations">The hard chat-turn cap.</param>
/// <param name="InvocationCount">Chat turns consumed so far.</param>
public sealed record EmbedLinkSummaryResponse(
    string Token,
    string WidgetId,
    string AgentId,
    Dictionary<string, string> Parameters,
    DateTime CreatedAt,
    DateTime ExpiresAt,
    int MaxInvocations,
    int InvocationCount)
{
    internal static EmbedLinkSummaryResponse From(EmbedLink link) => new(
        StripPrefix(link.Id),
        link.WidgetId,
        link.AgentId,
        link.Parameters,
        link.CreatedAt,
        link.ExpiresAt,
        link.MaxInvocations,
        link.InvocationCount);

    private static string StripPrefix(string? id) =>
        id is not null && id.StartsWith(EmbedLink.IdPrefix, StringComparison.Ordinal)
            ? id[EmbedLink.IdPrefix.Length..]
            : id ?? "";
}
