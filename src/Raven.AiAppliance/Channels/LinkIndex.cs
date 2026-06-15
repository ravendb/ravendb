namespace Raven.AiAppliance.Channels;

/// <summary>
/// Config-DB pointer that resolves a public embed-link <c>token</c> to the
/// per-app slug. Stored at <c>link-index/{token}</c> in the appliance config DB.
///
/// A routing pointer (like the legacy widget-index) and exists for the same reason: the public
/// <c>/embed/{token}</c> URL is unauthenticated and the bridge's default
/// <see cref="Raven.Client.Documents.IDocumentStore"/> targets the config DB,
/// but the <see cref="EmbedLink"/> doc lives in the *app's* database and the URL
/// carries only the token — so this pointer is the O(1) router from token to the
/// per-app DB to open.
///
/// Routing metadata, not a secret: it carries no parameters and is not the
/// credential (the <see cref="EmbedLink"/> in the app DB holds the TTL / cap /
/// bound parameters). Written at mint, deleted on revoke; a brief orphan is
/// harmless (the embed path re-validates the link doc exists).
/// </summary>
internal sealed class LinkIndex
{
    /// <summary>Doc-id prefix; the token is the suffix: <c>link-index/{token}</c>.</summary>
    internal const string IdPrefix = "link-index/";

    /// <summary><c>link-index/{token}</c>.</summary>
    public string? Id { get; set; }

    /// <summary>The per-app slug whose database holds <c>embed-links/{token}</c>.</summary>
    public string Slug { get; set; } = "";
}
