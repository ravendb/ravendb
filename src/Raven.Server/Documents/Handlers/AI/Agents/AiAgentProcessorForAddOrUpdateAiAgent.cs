using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Client.Json.Serialization;
using Raven.Server.Documents.AI;
using Raven.Server.Documents.Handlers.Processors;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.AST;
using Raven.Server.ServerWide.Commands.AI;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Server.Json.Sync;

namespace Raven.Server.Documents.Handlers.AI.Agents;
internal class AiAgentProcessorForAddOrUpdateAiAgent<TRequestHandler, TOperationContext> : AbstractDatabaseHandlerProcessor<TRequestHandler, TOperationContext>
    where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
    where TOperationContext : JsonOperationContext 
{
    public AiAgentProcessorForAddOrUpdateAiAgent([NotNull] TRequestHandler requestHandler) : base(requestHandler)
    {
    }

    public override async ValueTask ExecuteAsync()
    {
        using var token = RequestHandler.CreateHttpRequestBoundOperationToken();
        using var _ = ContextPool.AllocateOperationContext(out JsonOperationContext context);
        var options = await context.ReadForMemoryAsync(RequestHandler.RequestBodyStream(), "ai/agent", token.Token);
        
        var name = RequestHandler.GetStringQueryString("name", required: true);
        var configuration = JsonDeserializationClient.AiAgentConfiguration(options);

        ValidateConfiguration(context, configuration);

        var r = await ServerStore.SendToLeaderAsync(new AddOrUpdateAiAgentCommand(RequestHandler.DatabaseName, name, configuration, RequestHandler.GetRaftRequestIdFromQuery()), token.Token);
        
        RequestHandler.LogTaskToAudit($"Add/Update AI Agent '{name}'", r.Index, options);

        await RequestHandler.WaitForIndexNotificationAsync(r.Index);

        await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream(), token.Token))
        {
            var json = new DynamicJsonValue
            {
                [nameof(AiAgentConfigurationResult.RaftCommandIndex)] = r.Index
            };

            context.Write(writer, json);
        }
    }

    private static void ValidateConfiguration(JsonOperationContext context, AiAgentConfiguration configuration)
    {
        var scopeParams = configuration.Parameters;
        var toolParams = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var llmParams = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        foreach (var tool in configuration.Queries)
        {
            var q = QueryMetadata.ParseQuery(tool.Query, QueryType.Select);

            foreach (var p in q.Parameters)
            {
                toolParams.Add(p.Value);
            }

            string paramsSchema = ChatCompletionClient.GetSchemaForTool(tool.ParametersSchema, tool.ParametersSampleObject);
            var schema = context.Sync.ReadForMemory(paramsSchema, "tool-schema");
            if (schema.TryGet(ChatCompletionClient.Constants.JsonSchemaFields.Required, out BlittableJsonReaderArray required))
            {
                foreach (var arg in required)
                {
                    llmParams.Add(arg.ToString());
                }
            }
        }

        var missingToolSchema = llmParams.Except(toolParams).ToList();
        if (missingToolSchema.Count > 0)
            throw new InvalidOperationException($"Queries contain parameters that are not defined in the tool schema: '{string.Join(", ", missingToolSchema)}'");

        var requiredScope = toolParams.Except(llmParams).ToList();
        
        var missingScopeParams = requiredScope.Except(scopeParams).ToList();
        if (missingScopeParams.Count > 0)
            throw new InvalidOperationException($"Agent configuration missing parameters that is required by the tools: '{string.Join(", ", missingScopeParams)}'");

        var unusedParams = scopeParams.Except(requiredScope).ToList();
        if (unusedParams.Count > 0)
            throw new InvalidOperationException($"Agent configuration has unused parameters: '{string.Join(", ", unusedParams)}'");
    }
}
