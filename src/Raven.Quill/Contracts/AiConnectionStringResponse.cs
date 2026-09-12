using Raven.Client.Documents.Operations.AI;

namespace Raven.Quill.Contracts;

public enum AiConnectionStringUsageKind
{
    AiAgent,
    GenAi,
    EmbeddingsGeneration,
}

public sealed record AiConnectionStringUsage(
    AiConnectionStringUsageKind Kind,
    string? Identifier,
    string? Name,
    string? DatabaseName);

public sealed record AiConnectionStringResponse(
    AiConnectionString ConnectionString,
    IReadOnlyList<AiConnectionStringUsage> UsedBy);
