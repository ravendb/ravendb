namespace Raven.AiAppliance.Channels;

/// <summary>
/// Per-app channel document. Stored in the *app's own* RavenDB database
/// under the <c>Channels</c> collection (RavenDB derives the collection name
/// from the CLR type — class is named <c>Channel</c> so the persisted name
/// matches design §3.4 without overriding conventions). Doc id is
/// <c>channels/{channelId}</c>; only the bridge reads/writes these using its
/// admin client cert. (The appliance reserves the <c>@</c> prefix for its
/// system database alone — not for collection or document ids.) For the iFrame
/// channel the channelId is the random 128-bit <c>widgetId</c>; future channels
/// use a type-natural id (Telegram <c>botId</c>, WhatsApp <c>phoneNumberId</c>).
///
/// Channel-type-specific config (Telegram <c>BotToken</c>, iFrame theme,
/// JWT/anonymous-token secret, <c>ConversationDurationHours</c>,
/// <c>IdentifierSource</c>) and per-channel health metrics
/// (<c>LastSuccessfulPoll</c> / <c>LastErrorAt</c> / <c>ErrorCount</c>) will
/// move into a polymorphic <c>Config</c> sub-object when the first channel
/// that needs them ships (Telegram — RavenDB-26631). They are intentionally
/// absent here: the iFrame channel needs none of them.
/// </summary>
internal sealed class Channel
{
    /// <summary>The shared doc-id prefix — <c>Channel</c> owns the id scheme;
    /// endpoints and contracts reference this single constant.</summary>
    internal const string IdPrefix = "channels/";

    /// <summary><c>channels/{channelId}</c> (iFrame: <c>channels/{widgetId}</c>).</summary>
    public string? Id { get; set; }

    /// <summary>Channel kind. Persisted as its string name (<c>"IFrame"</c>);
    /// only <see cref="ChannelType.IFrame"/> ships in the 8-week demo.</summary>
    public ChannelType Type { get; set; }

    /// <summary>Operator-friendly label shown in the dashboard. Editable
    /// without touching the underlying <see cref="Id"/> / widgetId — so
    /// rename never breaks the customer's embed snippet.</summary>
    public string DisplayName { get; set; } = "";

    /// <summary>The <see cref="Schema.IAgentSchema.Identifier"/> this channel
    /// routes to. A channel routes to exactly one agent by design; one agent
    /// may be exposed by many channels.</summary>
    public string AgentId { get; set; } = "";

    /// <summary>Allowed origins for cross-origin script / CORS gating. Stored
    /// normalized to <c>scheme://authority</c> at provision time. Used to set
    /// the embed page's <c>Content-Security-Policy: frame-ancestors</c>.</summary>
    public string[] AllowedOrigins { get; set; } = [];

    /// <summary>Operator toggle to pause a channel without deleting its config
    /// (design §3.4). Defaults to <c>true</c>. A disabled iFrame channel makes
    /// <c>GET /embed/{widgetId}</c> return <c>410 Gone</c>; a future Telegram
    /// channel's polling service stops while disabled.</summary>
    public bool Enabled { get; set; } = true;

    /// <summary>UTC creation timestamp.</summary>
    public DateTime CreatedAt { get; set; }

    /// <summary>Doc id of the corresponding <see cref="ChannelBinding"/>:
    /// <c>channel-bindings/{slug}/{type}/{agentId}</c>. Stored for
    /// forward-compat: the delete flow reads it instead of recomputing it from
    /// slug + type + agent (which it does have in hand), so the binding-id
    /// format can evolve without breaking deletes of channels provisioned
    /// under an older format. The reverse link (binding -> channel)
    /// is intentionally NOT stored: <see cref="ChannelBinding.WidgetId"/>
    /// already exists and the channel doc id is <c>channels/{widgetId}</c> —
    /// trivially derivable.</summary>
    public string? BindingId { get; set; }
}
