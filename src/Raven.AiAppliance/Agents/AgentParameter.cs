namespace Raven.AiAppliance.Agents;

public sealed record AgentParameter
{
    public required string Name { get; init; }
    public required string Description { get; init; }
    public bool SendToModel { get; init; } = true;
    public AgentParameterPolicy Policy { get; init; } = AgentParameterPolicy.ForbidModelGeneration;
}

public enum AgentParameterPolicy
{
    Default,
    ForbidModelGeneration,
}
