namespace Raven.Quill.Contracts;

public sealed record AgentSummaryResponse(
    string AgentId,
    string Name,
    string? Model,
    bool Disabled,
    string[] Parameters,
    DateTime? LastInvokedAt,
    long Conversations,
    long Messages,
    long Tokens);
