using Raven.Client.Documents.Operations.AI;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.AI.Agents
{
    public sealed class AiInternalConversationResult
    {
        public static readonly AiInternalConversationResult Default = new AiInternalConversationResult { };

        public BlittableJsonReaderObject Response { get; set; }
        public AiUsage Usage { get; set; }
        public int ToolsIterations { get; set; }
    }
}
