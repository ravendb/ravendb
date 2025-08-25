namespace Raven.Server.Documents.AI.AiAssistant.Requests;

public class RefineGenAiPromptRequest : AiAssistRequestBase
{
    public string SourceCollectionName { get; set; }
    public string ContextGenerationScript { get; set; }
    public string Prompt { get; set; }
    public string OutputStructure { get; set; }
}
