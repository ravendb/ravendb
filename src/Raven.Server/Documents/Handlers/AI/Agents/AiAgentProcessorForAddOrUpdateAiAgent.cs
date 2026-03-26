using System;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Client.Json.Serialization;
using Raven.Server.Documents.Handlers.Processors;
using Raven.Server.ServerWide.Commands.AI;
using Sparrow.Json;
using Sparrow.Json.Parsing;

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

        RequestHandler.ServerStore.LicenseManager.AssertCanAddAiAgentTask(cfg);

        if (string.IsNullOrEmpty(cfg.Name))
            throw new ArgumentException("Ai Agent Name cannot be empty", nameof(cfg.Name));

        if (string.IsNullOrEmpty(cfg.Identifier))
            cfg.Identifier = EmbeddingsGenerationConfiguration.GenerateIdentifier(cfg.Name);
        
        AiAgentHelpers.AddDefaultValues(cfg, RequestHandler.Configuration.Ai);
        AddOrUpdateAiAgentCommand.ValidateConfiguration(context, cfg);

        var r = await ServerStore.SendToLeaderAsync(new AddOrUpdateAiAgentCommand(RequestHandler.DatabaseName, cfg, RequestHandler.GetRaftRequestIdFromQuery()),
            token.Token);

        RequestHandler.LogTaskToAudit(Web.RequestHandler.AddOrUpdateAiAgentConfiguration, r.Index, options);

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
}
