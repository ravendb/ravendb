namespace Raven.AiAppliance.Wizard;

/// <summary>
/// Per-app uniqueness primitive for channels (C2 from Copilot review
/// #4362803113). One document per <c>(slug, type, agentId)</c> tuple,
/// stored with a deterministic id <c>channel-bindings/{slug}/{type}/{agentId}</c>
/// on the per-app database. Written together with the corresponding
/// <see cref="Channel"/> in a <c>TransactionMode.ClusterWide</c>
/// session — RavenDB auto-creates an atomic guard on the binding doc id,
/// which serializes concurrent writers through Raft. The loser of the race
/// catches <see cref="Raven.Client.Exceptions.ClusterTransactionConcurrencyException"/>,
/// reads this binding, and returns its <see cref="WidgetId"/>.
///
/// The binding's id is the uniqueness key. <see cref="WidgetId"/> is the
/// stable customer-facing identifier (still 128 random bits — H1 from the
/// 2026-05-25 security review). Keeping these as separate documents
/// preserves the design §3.4 <c>@channels/{widgetId}</c> lookup path for
/// the future <c>/embed/{widgetId}</c> page.
/// </summary>
internal sealed class ChannelBinding
{
    /// <summary><c>channel-bindings/{slug}/{type}/{agentId}</c>.</summary>
    public string? Id { get; set; }

    /// <summary>The random 128-bit widgetId chosen for this binding. Stored
    /// here so concurrent losers can read it back and return it without
    /// touching the channel doc.</summary>
    public string WidgetId { get; set; } = "";

    /// <summary>UTC creation timestamp.</summary>
    public DateTime CreatedAt { get; set; }
}
