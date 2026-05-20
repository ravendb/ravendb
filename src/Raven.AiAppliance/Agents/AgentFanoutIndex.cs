namespace Raven.AiAppliance.Agents;

public sealed record AgentFanoutIndex
{
    public required string Name { get; init; }
    public required string MapExpression { get; init; }
}
