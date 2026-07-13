namespace Raven.Quill.Contracts;

/// <summary>
/// Mint a per-user embed link (RavenDB-26775) — <c>POST /api/apps/{slug}/embed-links</c>.
/// The customer's backend (which knows the logged-in user) calls this and embeds
/// the returned URL for that one user.
/// </summary>
/// <param name="AgentId">The agent to expose. Must have a provisioned iFrame
/// channel in the app DB (resolved via its <c>ChannelBinding</c>); otherwise 404.</param>
/// <param name="Parameters">Values for the agent's declared chat-scoped
/// parameters (e.g. <c>{ "Customer": "users/1" }</c>). Validated against the
/// agent at mint time and bound into the link — never supplied by the end-user's
/// browser. Any declared parameter missing/blank → 400; undeclared names dropped.</param>
/// <param name="TtlSeconds">Link lifetime in seconds. Optional; defaults to
/// <see cref="EmbedLinkLimits.DefaultTtlSeconds"/>. Bounded to
/// [<see cref="EmbedLinkLimits.MinTtlSeconds"/>, <see cref="EmbedLinkLimits.MaxTtlSeconds"/>].</param>
/// <param name="MaxInvocations">Hard cap on chat turns. Optional; defaults to
/// <see cref="EmbedLinkLimits.DefaultMaxInvocations"/>. Bounded to
/// [1, <see cref="EmbedLinkLimits.MaxMaxInvocations"/>].</param>
public sealed record MintEmbedLinkRequest(
    string AgentId,
    Dictionary<string, string>? Parameters = null,
    int? TtlSeconds = null,
    int? MaxInvocations = null);

/// <summary>Bounds + defaults for minted embed links. The TTL + invocation cap
/// are the primary abuse control (RavenDB-26775), so they are always bounded.</summary>
public static class EmbedLinkLimits
{
    public const int MinTtlSeconds = 60;                  // 1 minute
    public const int MaxTtlSeconds = 30 * 24 * 60 * 60;   // 30 days
    public const int DefaultTtlSeconds = 60 * 60;         // 1 hour

    public const int MaxMaxInvocations = 1_000_000;
    public const int DefaultMaxInvocations = 100;
}
