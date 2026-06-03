namespace Raven.AiAppliance.Channels;

/// <summary>
/// Config-DB pointer that resolves a public <c>widgetId</c> to the per-app
/// slug. Stored at <c>widget-index/{widgetId}</c> in the appliance config DB.
///
/// Why this exists: <c>GET /embed/{widgetId}</c> is unauthenticated and the
/// bridge's default <see cref="Raven.Client.Documents.IDocumentStore"/>
/// targets the config DB. The channel doc itself
/// (<c>@channels/{widgetId}</c>) lives in the *app's* database and does not
/// carry the slug, so the public URL — which only carries the widgetId — has
/// no way to know which per-app DB to open. This pointer is the O(1) router.
///
/// This is routing metadata, not a secret and not authentication. It is
/// written when a channel is provisioned and deleted when it is removed. A
/// brief orphan (crash between the per-app delete and this config-DB delete)
/// is harmless: re-provisioning overwrites it, and the embed page re-validates
/// that the channel doc still exists before serving.
/// </summary>
internal sealed class WidgetIndex
{
    /// <summary><c>widget-index/{widgetId}</c>.</summary>
    public string? Id { get; set; }

    /// <summary>The per-app slug whose database holds <c>@channels/{widgetId}</c>.</summary>
    public string Slug { get; set; } = "";
}
