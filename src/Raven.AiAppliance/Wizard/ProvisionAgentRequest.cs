namespace Raven.AiAppliance.Wizard;

/// <summary>
/// W7 input — Stage C.2 step 11 of the wizard (design §1.3): provision an AI
/// agent against an already-created per-app database, referencing a previously-
/// POSTed AI connection string by name. The connection string itself is created
/// via <c>POST /api/apps/{slug}/ai/connection-strings</c> (see
/// <see cref="Endpoints.AiConnectionStringsEndpoints"/>) — the wizard shows
/// "pick existing OR add new" for that step, and this endpoint just references
/// the chosen name.
/// </summary>
/// <param name="ConnectionStringName">Name of the AI connection string on the
/// per-app database. The agent's <c>AiAgentConfiguration.ConnectionStringName</c>
/// is set to this value; the operator can swap LLM providers by deleting the
/// agent and re-provisioning against a different CS name.</param>
/// <param name="Framing">Operator-chosen agent framing (e.g. "customer-support").
/// Logged only for now — persisting on the App doc is a future slice. Does not
/// drive schema selection yet either: the 8-week demo registers the single
/// DI-supplied <see cref="Schema.IAgentSchema"/> regardless. Multi-framing
/// schema picking + framing persistence both live under design §1.3 step 9
/// "AI-suggest" paths.</param>
internal sealed record ProvisionAgentRequest(string ConnectionStringName, string? Framing = null);
