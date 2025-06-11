using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations.AI.AiAgent;
using Raven.Client.Json.Serialization;
using Raven.Server.Documents.Handlers.Processors;
using Raven.Server.ServerWide.Commands.AI;
using Raven.Server.Web;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Handlers.AI.AiAgent;
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
        
        var name = RequestHandler.GetStringQueryString("agent", required: true);
        var cfg = JsonDeserializationClient.AiAgentConfiguration(options);

        var r = await ServerStore.SendToLeaderAsync(new AddOrUpdateAiAgentCommand(RequestHandler.DatabaseName, name, cfg, RequestHandler.GetRaftRequestIdFromQuery()), token.Token);
        
        RequestHandler.LogTaskToAudit($"Create AI Agent '{name}'", r.Index, options);

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
}
