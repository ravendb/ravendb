namespace Raven.Server.Documents.AI.AiAssistant.Responses;

public class AiAssistResponseBase
{
    public AiResponseStatus ResponseStatus { get; set; }
    public int InputTokenCount { get; set; }
    public int OutputTokenCount { get; set; }
}
