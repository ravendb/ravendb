using Raven.Server.Documents.AI.AiAssistant.Handlers;

namespace Raven.Server.Documents.AI.AiAssistant;

public abstract class AiAssistRequestBase : AiAssistantRequestAuthentication
{
    public RequestType RequestType { get; set; }
}
