using System.Diagnostics.CodeAnalysis;

namespace Raven.AiAppliance.Schema;

public sealed class AgentSchemaRegistry : IAgentSchemaRegistry
{
    private readonly Dictionary<string, IAgentSchema> _byId;

    public AgentSchemaRegistry(IEnumerable<IAgentSchema> schemas)
    {
        _byId = new Dictionary<string, IAgentSchema>(StringComparer.OrdinalIgnoreCase);
        foreach (var schema in schemas)
        {
            if (!_byId.TryAdd(schema.Identifier, schema))
                throw new InvalidOperationException(
                    $"Two IAgentSchema implementations share the identifier '{schema.Identifier}'. " +
                    $"Identifiers must be unique across the DI container.");
        }
        All = _byId.Values.ToArray();
    }

    public IReadOnlyList<IAgentSchema> All { get; }

    public bool TryGet(string identifier, [NotNullWhen(true)] out IAgentSchema? schema) =>
        _byId.TryGetValue(identifier, out schema);

    public IAgentSchema Require(string identifier) =>
        TryGet(identifier, out var s)
            ? s
            : throw new KeyNotFoundException($"No agent schema registered with identifier '{identifier}'.");
}
