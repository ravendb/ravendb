namespace Raven.Server.Documents.AI.AiAssistant.Responses;

public class AiAssistResponseBase
{
    public AiAssistantResponseStatus ResponseStatus { get; set; }
    public int InputTokenCount { get; set; }
    public int OutputTokenCount { get; set; }
    public int UsagePercentage { get; set; }
}
