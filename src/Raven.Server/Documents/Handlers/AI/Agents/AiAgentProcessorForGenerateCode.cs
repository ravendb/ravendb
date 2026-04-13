using System;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Server.Documents.Handlers.Processors;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.AI.Agents
{
    internal class AiAgentProcessorForGenerateCode<TRequestHandler, TOperationContext> : AbstractDatabaseHandlerProcessor<TRequestHandler, TOperationContext>
        where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
        where TOperationContext : JsonOperationContext
    {
        public AiAgentProcessorForGenerateCode([NotNull] TRequestHandler requestHandler) : base(requestHandler)
        {
        }

        public override async ValueTask ExecuteAsync()
        {
            using var token = RequestHandler.CreateHttpRequestBoundOperationToken();
            var identifier = RequestHandler.GetStringQueryString("agentId");
            var lang = RequestHandler.GetStringQueryString("language");

            AiAgentConfiguration agent = null;

            using (ServerStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext ctx))
            using (ctx.OpenReadTransaction())
            using (var record = ServerStore.Cluster.ReadRawDatabaseRecord(ctx, RequestHandler.DatabaseName))
            {
                if (string.IsNullOrEmpty(identifier) == false)
                {
                    if (record.TryGetAiAgent(identifier, out agent) == false)
                    {
                        throw new InvalidOperationException($"Agent '{identifier}' doesn't exist");
                    }
                }
            }

            var generatedCode = lang.ToLower() switch
            {
                "c#" => new CSharpCodeGenerator().GenerateFullFile(agent, "agent"),
                "javascript" => new NodejsCodeGenerator().GenerateFullFile(agent, "agent"),
                "python" => new PythonCodeGenerator().GenerateFullFile(agent, "agent"),
                _ => throw new ArgumentException($"Unsupported language '{lang}'. Supported languages are: C#, JavaScript, Python.")
            };

            // if name is null or empty - return all agents
            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream(), token.Token))
            {
                writer.WriteStartObject();
                writer.WritePropertyName("GeneratedCode");
                writer.WriteString(generatedCode);
                writer.WriteEndObject();
            }
        }
    }
}
