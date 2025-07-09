using JetBrains.Annotations;

namespace Raven.Server.Documents.Handlers.AI.Agents;

internal class AiAgentProcessor : AbstractAiAgentProcessor
{
    public AiAgentProcessor([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }
}
