using Raven.Server.Documents.AI.AiAssistant.Handlers;

namespace Raven.Server.Documents.AI.AiAssistant;

public sealed class RefineTextGenAi : AiAssistRequestBase
{
    public string UserInput { get; set; }
    public string Script { get; set; }
}
