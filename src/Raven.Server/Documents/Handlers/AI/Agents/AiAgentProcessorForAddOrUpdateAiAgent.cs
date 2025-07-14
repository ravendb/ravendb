using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Client.Json.Serialization;
using Raven.Server.Documents.AI;
using Raven.Server.Documents.Handlers.Processors;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.AST;
using Raven.Server.ServerWide.Commands.AI;
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
        
        var cfg = JsonDeserializationClient.AiAgentConfiguration(options);
        if (string.IsNullOrEmpty(cfg.Name))
            throw new ArgumentException("Ai Agent Name cannot be empty", nameof(cfg.Name));

        if (string.IsNullOrEmpty(cfg.Identifier))
            cfg.Identifier = EmbeddingsGenerationConfiguration.GenerateIdentifier(cfg.Name);
        
        ValidateConfiguration(context, cfg);
        
        var r = await ServerStore.SendToLeaderAsync(new AddOrUpdateAiAgentCommand(RequestHandler.DatabaseName, cfg, RequestHandler.GetRaftRequestIdFromQuery()),
            token.Token);

        RequestHandler.LogTaskToAudit($"Add/Update AI Agent '{cfg.Identifier}'", r.Index, options);

        await RequestHandler.WaitForIndexNotificationAsync(r.Index);

        await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream(), token.Token))
        {
            var json = new DynamicJsonValue
            {
                [nameof(AiAgentConfigurationResult.Identifier)] = cfg.Identifier,
                [nameof(AiAgentConfigurationResult.RaftCommandIndex)] = r.Index
            };

            context.Write(writer, json);
        }
    }

    private static void ValidateConfiguration(JsonOperationContext context, AiAgentConfiguration configuration)
    {
        var reduction = configuration.ChatReduction;
        if (reduction != null && reduction.Tokens != null && reduction.Truncate != null)
            throw new InvalidOperationException($"'{nameof(configuration.ChatReduction)}' cannot have both '{nameof(reduction.Tokens)}' and '{nameof(reduction.Truncate)}'. Please specify at most one of these strategies.");

        var scopeParams = configuration.Parameters;
        foreach (var tool in configuration.Queries)
        {
            var q = QueryMetadata.ParseQuery(tool.Query, QueryType.Select);
            var queryParams = new HashSet<string>(q.Parameters.Select(x => x.Value));
            queryParams.ExceptWith(scopeParams);

            string paramsSchema = ChatCompletionClient.GetSchemaForTool(tool.ParametersSchema, tool.ParametersSampleObject);
            var schema = context.Sync.ReadForMemory(paramsSchema, "tool-schema");
            if (schema.TryGet(ChatCompletionClient.Constants.JsonSchemaFields.Required, out BlittableJsonReaderArray required))
            {
                foreach (var arg in required)
                {
                    string queryArg = arg.ToString();
                    if (scopeParams.Contains(queryArg))
                        throw new InvalidOperationException($"Parameter {queryArg} is defined on both the agent level and the query level for {tool.Name}");

                    queryParams.Remove(queryArg);
                }
            }

            if (queryParams.Count > 0)
                throw new InvalidOperationException(
                    $"Tool query '{tool.Name}' contains parameters that are not defined in the agent configuration: '{string.Join(", ", queryParams)}'");
        }
    }
}
