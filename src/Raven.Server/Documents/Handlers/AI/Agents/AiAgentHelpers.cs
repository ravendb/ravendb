using System;
using System.Linq;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Server.Config.Categories;
using Raven.Server.Documents.AI;
using Sparrow.Json;
using Sparrow.Server.Json.Sync;

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

    // Deliberately kept out of AddOrUpdateAiAgentCommand.ValidateConfiguration, which the command runs while
    // applying an already committed Raft entry: a rule that only some versions enforce would then let the same
    // entry apply on one node and fail on another. Called from the request handlers instead, where a rejection
    // still reaches the client and nothing has been committed.
    public static void AssertAgentParametersAreNotToolArguments(JsonOperationContext context, AiAgentConfiguration configuration)
    {
        if (configuration.Parameters == null || configuration.Queries == null)
            return;

        var scopeParams = configuration.Parameters.Select(x => x.Name).ToHashSet();
        if (scopeParams.Count == 0)
            return;

        foreach (var tool in configuration.Queries)
        {
            var paramsSchema = ChatCompletionClient.GetSchemaForTool(tool.ParametersSchema, tool.ParametersSampleObject);
            var schema = context.Sync.ReadForMemory(paramsSchema, "tool-schema");

            if (schema.TryGet(ChatCompletionClient.Constants.JsonSchemaFields.Properties, out BlittableJsonReaderObject properties) == false)
                continue;

            foreach (var toolArgument in properties.GetPropertyNames())
            {
                if (scopeParams.Contains(toolArgument))
                    throw new InvalidOperationException(
                        $"Parameter {toolArgument} is defined on both the agent level and the query level for {tool.Name}");
            }
        }
    }
}
