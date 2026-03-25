using System;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Client.Json.Serialization;
using Raven.Server.Documents.Handlers.Processors;
using Raven.Server.Monitoring.Snmp.Objects.Database;
using Raven.Server.ServerWide.Commands.AI;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;

namespace Raven.Server.Documents.Handlers.AI.Agents;

internal class AiAgentProcessorForDeleteAiAgent<TRequestHandler, TOperationContext> : AbstractDatabaseHandlerProcessor<TRequestHandler, TOperationContext>
    where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
    where TOperationContext : JsonOperationContext 
{
    public AiAgentProcessorForDeleteAiAgent([NotNull] TRequestHandler requestHandler) : base(requestHandler)
    {
    }

    public override async ValueTask ExecuteAsync()
    {
        using var token = RequestHandler.CreateHttpRequestBoundOperationToken();
        var identifier = RequestHandler.GetStringQueryString("agentId", required: true);
        var r = await ServerStore.SendToLeaderAsync(new DeleteAiAgentCommand(RequestHandler.DatabaseName, identifier, RequestHandler.GetRaftRequestIdFromQuery()), token.Token);

        if (RavenLogManager.Instance.IsAuditEnabled)
            RequestHandler.LogAuditForDatabase("DELETE", $"AiAgentConfiguration '{identifier}'");

        await RequestHandler.WaitForIndexNotificationAsync(r.Index);

        using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
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
