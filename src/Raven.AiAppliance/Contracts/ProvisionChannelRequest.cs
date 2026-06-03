using Raven.AiAppliance.Channels;

namespace Raven.AiAppliance.Contracts;

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
/// must match a registered <see cref="Schema.IAgentSchema.Identifier"/>.</param>
/// <param name="AllowedOrigins">Allowed origins for the embed page's
/// CORS / CSP gating.</param>
/// <param name="DisplayName">Optional operator-friendly label. Defaults to
/// <see cref="Type"/> when omitted.</param>
public sealed record ProvisionChannelRequest(
    ChannelType? Type,
    string AgentId,
    string[] AllowedOrigins,
    string? DisplayName = null);
