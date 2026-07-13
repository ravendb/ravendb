namespace Raven.Quill.Contracts;

/// <summary>Channel-create result.</summary>
/// <param name="WidgetId">The channel's public widget id.</param>
/// <param name="Existing">True when the (slug, type, agent) channel already
/// existed — the request's <c>allowedOrigins</c>/<c>displayName</c> were NOT
/// applied (provision is create-only; edit via <c>PUT /channels/{id}</c>).
/// Set on both the fast-path and the concurrency race-loser returns.</param>
public sealed record ProvisionChannelResponse(string WidgetId, bool Existing = false);
