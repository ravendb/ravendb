namespace Raven.AiAppliance.Wizard;

/// <summary>
/// W8 input — Stage C.2 step 12 of the wizard (design §1.3 "Enable channels"):
/// register a channel instance against an already-provisioned app DB.
/// </summary>
/// <param name="Type">Channel kind. Only <c>"iframe"</c> is supported in the
/// 8-week demo; Telegram + WhatsApp are deferred (design §3.6 / §3.7).</param>
/// <param name="AgentId">Identifier of the agent this channel routes to —
/// must match a registered <see cref="Schema.IAgentSchema.Identifier"/>.</param>
/// <param name="AllowedOrigins">Allowed origins for the future embed page's
/// CORS / CSP gating.</param>
/// <param name="DisplayName">Optional operator-friendly label. Defaults to
/// <see cref="Type"/> when omitted.</param>
internal sealed record ProvisionChannelRequest(
    string Type,
    string AgentId,
    string[] AllowedOrigins,
    string? DisplayName = null);
