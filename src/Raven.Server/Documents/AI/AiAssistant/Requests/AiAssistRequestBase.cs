
namespace Raven.Server.Documents.AI.AiAssistant.Requests;

public abstract class AiAssistRequestBase : AiAssistantRequestAuthentication
{
    public AiOperationType OperationType { get; set; }
}
