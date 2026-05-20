using System.Diagnostics.CodeAnalysis;

namespace Raven.AiAppliance.Schema;

public interface IAgentSchemaRegistry
{
    IReadOnlyList<IAgentSchema> All { get; }
    bool TryGet(string identifier, [NotNullWhen(true)] out IAgentSchema? schema);
    IAgentSchema Require(string identifier);
}
