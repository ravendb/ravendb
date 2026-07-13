using Raven.Quill.Channels;

namespace Raven.Quill.Contracts;

/// <summary>
/// Channel-create input — Stage C.2 step 12 of the wizard (design §1.3
/// "Enable channels"): register a channel instance against an
/// already-provisioned app DB.
/// </summary>
/// <param name="Type">Channel kind. Bound case-insensitively (e.g. <c>"iframe"</c>
/// -> <see cref="ChannelType.IFrame"/>); only <see cref="ChannelType.IFrame"/> is
/// implemented in the 8-week demo (Telegram/WhatsApp -> 501). Required: a missing
/// <c>type</c> binds to <c>null</c> and is rejected with 400 rather than silently
/// defaulting to <see cref="ChannelType.IFrame"/>.</param>
/// <param name="AgentId">Identifier of the agent this channel routes to —
/// must match an agent provisioned in the app's database.</param>
/// <param name="AllowedOrigins">Allowed origins for the embed page's
/// CSP <c>frame-ancestors</c> and the chat POST's M1b Origin check;
/// normalized to <c>scheme://authority</c> on persist. Required — omitting
/// the property is rejected with 400 so an open embed is always an explicit
/// choice. An explicit empty list is that opt-in (M1, decided 2026-06-04):
/// no <c>frame-ancestors</c> header is emitted and the Origin check is
/// skipped — embeddable/postable from any site.</param>
/// <param name="DisplayName">Optional operator-friendly label. Defaults to
/// <see cref="Type"/> when omitted.</param>
public sealed record ProvisionChannelRequest(
    ChannelType? Type,
    string AgentId,
    string[]? AllowedOrigins,
    string? DisplayName = null);
