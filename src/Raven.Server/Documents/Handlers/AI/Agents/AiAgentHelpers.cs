using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Server.Config.Categories;

namespace Raven.Server.Documents.Handlers.AI.Agents;

public static class AiAgentHelpers
{
    public static void AddDefaultValues(AiAgentConfiguration configuration, AiConfiguration aiConfig)
    {
        var reduction = configuration.ChatTrimming;
        if (reduction?.Tokens != null)
        {
            if (string.IsNullOrEmpty(reduction.Tokens.SummarizationTaskBeginningPrompt))
                reduction.Tokens.SummarizationTaskBeginningPrompt = aiConfig.SummarizationTaskBeginningPrompt;

            if (string.IsNullOrEmpty(reduction.Tokens.SummarizationTaskEndPrompt))
                reduction.Tokens.SummarizationTaskEndPrompt = aiConfig.SummarizationTaskEndPrompt;

            if (string.IsNullOrEmpty(reduction.Tokens.ResultPrefix))
                reduction.Tokens.ResultPrefix = aiConfig.SummarizationResultPrefix;
        }
    }   
}
