namespace Raven.AiAppliance.Wizard;

/// <summary>
/// Per-app channel document. Stored in the *app's own* RavenDB database
/// under the <c>Channels</c> collection (RavenDB derives the collection name
/// from the CLR type — class is named <c>Channel</c> so the persisted name
/// matches design §3.4 without overriding conventions). Doc id is the
/// explicit <c>@channels/{widgetId}</c> form (the <c>@</c> prefix keeps these
/// out of the user-facing collection views; only the bridge reads/writes them
/// using its admin client cert).
///
/// This is the **minimal-for-8-week-demo** subset of the §3.4 shape — just
/// what W8 needs to provision an iFrame channel. The richer fields
/// (<c>Enabled</c>, <c>Config</c> with JwtSecret / Theme,
/// <c>ConversationDurationHours</c>, <c>IdentifierSource</c>, <c>UpdatedAt</c>)
/// are out of scope for this slice — they ship when the dashboard's
/// "Channels &amp; Adapters" tab needs them.
/// </summary>
internal sealed class Channel
{
    /// <summary><c>@channels/{widgetId}</c>.</summary>
    public string? Id { get; set; }

    /// <summary><c>"IFrame"</c> for the 8-week demo. Future: <c>"Telegram"</c>,
    /// <c>"WhatsApp"</c> (design §3.4 table).</summary>
    public string Type { get; set; } = "";

    /// <summary>Operator-friendly label shown in the dashboard. Editable
    /// without touching the underlying <see cref="Id"/> / widgetId — so
    /// rename never breaks the customer's embed snippet.</summary>
    public string DisplayName { get; set; } = "";

    /// <summary>The <see cref="Schema.IAgentSchema.Identifier"/> this channel
    /// routes to. One channel binds to exactly one agent for the POC; one
    /// agent may be exposed by many channels (design §3.4).</summary>
    public string AgentId { get; set; } = "";

    /// <summary>Allowed origins for cross-origin script / CORS gating. Stored
    /// as-is from the operator; CORS enforcement on the future
    /// <c>/embed/{widgetId}</c> page is out of scope for this slice.</summary>
    public string[] AllowedOrigins { get; set; } = [];

    /// <summary>UTC creation timestamp.</summary>
    public DateTime CreatedAt { get; set; }
}
