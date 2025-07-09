using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Server.Documents.Handlers.Processors;
using Raven.Server.ServerWide;
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
        var identifier = RequestHandler.GetStringQueryString("id", required: false);

        List<AiAgentConfiguration> agents;

        using (ServerStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext ctx))
        using (ctx.OpenReadTransaction())
        using (var record = ServerStore.Cluster.ReadRawDatabaseRecord(ctx, RequestHandler.DatabaseName))
        {
            if (string.IsNullOrEmpty(identifier) == false)
            {
                if (record.TryGetAiAgent(identifier, out var configuration) == false)
                    throw new ArgumentException($"AI Agent '{identifier}' doesn't exists");

                using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
                await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream(), token.Token))
                {
                    var obj = context.ReadObject(configuration.ToJson(), "get-ai-agent");
                    writer.WriteObject(obj);
                }

                return;
            }

            agents = record.AiAgents;
        }

        // if name is null or empty - return all agents
        using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
        await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream(), token.Token))
        {
            writer.WriteStartObject();
            writer.WritePropertyName(nameof(RawDatabaseRecord.AiAgents));
            writer.WriteStartArray();
            var first = true;
            foreach (var agent in agents)
            {
                if (first == false)
                    writer.WriteComma();

                first = false;

                writer.WriteObject(context.ReadObject(agent.ToJson(), "ai-agent"));
            }
            writer.WriteEndArray();
            writer.WriteEndObject();
        }
    }
}
