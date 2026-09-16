using Raven.Client.Documents.Operations.AI.Agents;

namespace Raven.Quill.Contracts;

public sealed record AgentParameterSummary(
    string Name,
    string? Description,
    AiAgentParameterValueType Type);

public sealed record AgentSummaryResponse(
    string AgentId,
    string Name,
    string? Model,
    bool Disabled,
    AgentParameterSummary[] Parameters,
    DateTime? LastInvokedAt,
    long Conversations,
    long Messages,
    long Tokens);
