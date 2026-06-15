namespace Raven.AiAppliance.Channels;

/// <summary>
/// An API-minted, per-user embed grant (RavenDB-26775). Replaces the old
/// static <c>/embed/{widgetId}</c> exposure: the customer's backend mints one
/// of these per end-user via <c>POST /api/apps/{slug}/embed-links</c>, and the
/// returned opaque <c>token</c> is the bearer credential in the iframe URL.
///
/// Stored in the *app's own* database (like <see cref="Channel"/>). The
/// <c>token</c> is generated exactly like a <c>chats/{guid}</c> id
/// (<c>Guid.NewGuid().ToString("N")</c>) and IS the doc-id suffix
/// (<c>embed-links/{token}</c>), stored raw — 122 random bits, unguessable, and
/// the app DB is encrypted at rest. A sibling <see cref="LinkIndex"/> pointer in
/// the config DB resolves the token to this app (a routing pointer like the legacy widget-index).
///
/// The grant closes the three holes of the static model: agent
/// <see cref="Parameters"/> are bound here at mint time (never client-supplied,
/// so no <c>?customerId=</c> impersonation); <see cref="ExpiresAt"/> +
/// <see cref="MaxInvocations"/> bound abuse/cost; and <see cref="ConversationId"/>
/// is server-owned (the link owns its one conversation), so the client never
/// supplies a conversation id.
/// </summary>
internal sealed class EmbedLink
{
    /// <summary>Doc-id prefix; the token is the suffix: <c>embed-links/{token}</c>.</summary>
    internal const string IdPrefix = "embed-links/";

    /// <summary>A token is exactly a <c>Guid.NewGuid().ToString("N")</c> — 32
    /// lowercase hex chars. The public path validates this before building the
    /// doc id so a crafted token can't be used to probe other documents.</summary>
    internal static bool IsWellFormedToken(string? token)
    {
        if (token is not { Length: 32 })
            return false;

        foreach (var c in token)
        {
            if (c is (< '0' or > '9') and (< 'a' or > 'f'))
                return false;
        }

        return true;
    }

    /// <summary><c>embed-links/{token}</c>.</summary>
    public string? Id { get; set; }

    /// <summary>The channel (config anchor) this link targets — its
    /// <c>AllowedOrigins</c> / <c>Enabled</c> / agent are read live on each turn,
    /// so an operator edit/disable affects already-minted links.</summary>
    public string WidgetId { get; set; } = "";

    /// <summary>The agent identifier this link routes to (denormalized from the
    /// channel for listing/audit).</summary>
    public string AgentId { get; set; } = "";

    /// <summary>The agent's declared chat-scoped parameters, validated and bound
    /// at mint time (e.g. <c>{ "Customer": "users/1" }</c>). Forwarded to the
    /// agent verbatim on every turn; never overridable by the chat request body.</summary>
    public Dictionary<string, string> Parameters { get; set; } = new();

    /// <summary>UTC instant after which the link is dead (TTL). Enforced at
    /// runtime; also mirrored into the <c>@expires</c> metadata so RavenDB's
    /// Expiration feature can sweep spent links.</summary>
    public DateTime ExpiresAt { get; set; }

    /// <summary>Hard cap on chat turns this link may run. The structural rate
    /// limit — set per-link by whoever mints it.</summary>
    public int MaxInvocations { get; set; }

    /// <summary>Chat turns consumed so far; atomically incremented (optimistic
    /// concurrency) before each turn runs, so the cap holds under concurrency.</summary>
    public int InvocationCount { get; set; }

    /// <summary>The <c>chats/{guid}</c> conversation this link runs against,
    /// minted on the first turn and pinned thereafter. Server-owned — the client
    /// never supplies a conversation id.</summary>
    public string? ConversationId { get; set; }

    /// <summary>Operator/customer kill switch; a revoked link is <c>410 Gone</c>.</summary>
    public bool Revoked { get; set; }

    /// <summary>UTC creation timestamp.</summary>
    public DateTime CreatedAt { get; set; }
}
