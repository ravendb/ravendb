namespace Raven.AiAppliance.Wizard;

/// <summary>
/// W7 input — Stage C.2 step 11 of the wizard (design §1.3): provision an AI
/// agent against an already-created per-app database.
/// </summary>
/// <param name="Framing">Operator-chosen agent framing (e.g. "customer-support").
/// Logged + recorded on the App doc; does not drive schema selection yet — the
/// 8-week demo registers the single DI-supplied <see cref="Schema.IAgentSchema"/>
/// regardless. Multi-framing schema picking is a follow-up slice (design §1.3
/// step 9 "AI-suggest" paths).</param>
internal sealed record ProvisionAgentRequest(string? Framing);
