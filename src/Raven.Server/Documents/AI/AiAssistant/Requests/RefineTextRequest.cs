
namespace Raven.Server.Documents.AI.AiAssistant.Requests;

public sealed class RefineTextRequest : AiAssistRequestBase
{
    public string Text { get; set; }
}
