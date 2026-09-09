using Raven.Client.Documents.Operations.AI.Agents;

namespace Raven.Quill.Contracts;

public sealed record SuggestAgentResponse(
    IReadOnlyList<AiAgentConfiguration> Configurations,
    IReadOnlyList<string> Rationale,
    string Status);
