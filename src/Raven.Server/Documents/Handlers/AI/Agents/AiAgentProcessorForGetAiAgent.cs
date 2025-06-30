using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Server.Documents.Handlers.Processors;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.AI.Agents;

internal class AiAgentProcessorForGetAiAgent<TRequestHandler, TOperationContext> : AbstractDatabaseHandlerProcessor<TRequestHandler, TOperationContext>
    where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
    where TOperationContext : JsonOperationContext
{
    public AiAgentProcessorForGetAiAgent([NotNull] TRequestHandler requestHandler) : base(requestHandler)
    {
    }

    public override async ValueTask ExecuteAsync()
    {
        using var token = RequestHandler.CreateHttpRequestBoundOperationToken();
        var name = RequestHandler.GetStringQueryString("name");

        Dictionary<string, AiAgentConfiguration> agents;
        using (ServerStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext ctx))
        using (ctx.OpenReadTransaction())
        using (var record = ServerStore.Cluster.ReadRawDatabaseRecord(ctx, RequestHandler.DatabaseName))
        {
            agents = record.AiAgents;
        }

        if (string.IsNullOrEmpty(name) == false)
        {
            if (agents.TryGetValue(name, out var configuration) == false)
                throw new ArgumentException($"AI Agent '{name}' doesn't exists");

            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream(), token.Token))
            {
                var obj = context.ReadObject(configuration.ToJson(), "get-ai-agent");
                writer.WriteObject(obj);
            }

            return;
        }

        // if name is null or empty - return all agents
        using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
        await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream(), token.Token))
        {
            writer.WriteStartObject();
            var first = true;
            foreach (var agent in agents)
            {
                if (first == false)
                    writer.WriteComma();

                first = false;

                writer.WritePropertyName(agent.Key);
                writer.WriteObject(context.ReadObject(agent.Value.ToJson(), "ai-agent"));
            }
            writer.WriteEndObject();
        }
    }
}
