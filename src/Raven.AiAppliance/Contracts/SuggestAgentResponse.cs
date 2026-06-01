using Raven.Client.Documents.Operations.AI.Agents;

namespace Raven.AiAppliance.Contracts;

/// <summary>
/// AI-suggested agent draft candidate(s). <see cref="Configurations"/> is empty when
/// <see cref="Status"/> is not <c>Success</c>. Generate-only: a candidate populates the
/// editable agent Review form; provisioning stays with the existing per-app setup flow.
/// </summary>
public sealed record SuggestAgentResponse(
    IReadOnlyList<AiAgentConfiguration> Configurations,
    IReadOnlyList<string> Rationale,
    string Status);
