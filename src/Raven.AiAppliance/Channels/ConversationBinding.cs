namespace Raven.AiAppliance.Channels;

/// <summary>
/// Maps an opaque client-held conversation token to the real (hidden)
/// <c>chats/</c> conversation id — the A2 fix for the public embed
/// (RavenDB-26700 auth follow-up). One doc per conversation, stored at
/// <c>conversation-bindings/{widgetId}/{key}</c> on the per-app database.
/// The widgetId inside the id makes a token minted under one widget a
/// structural miss under any other, and the random 128-bit key
/// (<see cref="RandomIds"/>) is what keeps the public token unguessable —
/// the real conversation id never leaves the server.
///
/// Same idiom as <see cref="ChannelBinding"/>: written in a
/// <c>TransactionMode.ClusterWide</c> session whose atomic guard serializes
/// concurrent writers; the race loser reads the winner's doc back. For the
/// iFrame the key is random, so the race can't happen — the guard does real
/// work only for a future deterministic-key consumer (a channel keying
/// conversations by platform identity; Telegram as currently drafted derives
/// its conversation ids directly and does not use bindings).
///
/// <see cref="ExpiresAt"/> is the TOKEN-VALIDITY window, read-validated by
/// <see cref="ConversationBindings.TryResolveAsync"/> — it is NOT document
/// expiration. (Session retention via <c>@expires</c> is a deferred
/// RavenDB-26700 follow-up; bindings deliberately carry no <c>@expires</c>.)
/// An existing-but-expired binding stays expired; refresh-on-expired is a
/// future deterministic-key consumer's concern.
/// </summary>
internal sealed class ConversationBinding
{
    /// <summary>The shared doc-id prefix — endpoints and tests reference this
    /// single constant (mirrors <see cref="Channel.IdPrefix"/>).</summary>
    internal const string IdPrefix = "conversation-bindings/";

    /// <summary>Builds <c>conversation-bindings/{widgetId}/{key}</c> — the
    /// one place the binding id scheme is defined.</summary>
    internal static string MakeId(string widgetId, string key) => $"{IdPrefix}{widgetId}/{key}";

    /// <summary><c>conversation-bindings/{widgetId}/{key}</c>.</summary>
    public string? Id { get; set; }

    /// <summary>The hidden real conversation doc id (<c>chats/...</c>).</summary>
    public string ConversationId { get; set; } = "";

    /// <summary>Owning channel — derivable from <see cref="Id"/>, stored for
    /// debuggability (mirrors <see cref="ChannelBinding.WidgetId"/>).</summary>
    public string WidgetId { get; set; } = "";

    /// <summary>UTC creation timestamp.</summary>
    public DateTime CreatedAt { get; set; }

    /// <summary>Token-validity deadline (UTC) — see class remarks.</summary>
    public DateTime ExpiresAt { get; set; }
}
