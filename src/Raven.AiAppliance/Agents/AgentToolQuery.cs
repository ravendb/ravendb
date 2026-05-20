namespace Raven.AiAppliance.Agents;

public sealed record AgentToolQuery
{
    public required string Name { get; init; }
    public required string Description { get; init; }
    public required string Query { get; init; }
    public string ParametersSampleJson { get; init; } = "{}";
    public bool AddToInitialContext { get; init; }
    public bool AllowModelQueries { get; init; }
}
