namespace Raven.AiAppliance.Wizard;

/// <summary>
/// W7 input — Stage C.2 step 11 of the wizard (design §1.3): provision an AI
/// agent against an already-created per-app database.
/// </summary>
/// <param name="Framing">Operator-chosen agent framing (e.g. "customer-support").
/// Logged only for now — persisting on the App doc is a future slice. Does not
/// drive schema selection yet either: the 8-week demo registers the single
/// DI-supplied <see cref="Schema.IAgentSchema"/> regardless. Multi-framing
/// schema picking + framing persistence both live under design §1.3 step 9
/// "AI-suggest" paths.</param>
internal sealed record ProvisionAgentRequest(string? Framing);
