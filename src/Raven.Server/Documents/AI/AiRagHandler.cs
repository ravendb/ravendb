using System;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Json.Serialization;
using Raven.Server.Documents.AI.AiGen;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.AI;

public class AiRagHandler : DatabaseRequestHandler
{
    [RavenAction("/databases/*/ai/rag", "POST", AuthorizationStatus.ValidUser, EndpointType.Read, DisableOnCpuCreditsExhaustion = true)]
    public async Task Rag()
    {
        using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
        using (var token = CreateHttpRequestBoundOperationToken())
        {
            var options = await context.ReadForMemoryAsync(RequestBodyStream(), "ai/rag", token.Token);
            var cfg = JsonDeserializationClient.AiRagConfiguration(options);
            var conStr = GetAiConnectionString(cfg.ConnectionStringName);

            (string url, string model, string apikey) = conStr.GetSettings();

            string schemaOrSampleObject = cfg.OutputSchema ?? throw new InvalidOperationException("Missing output schema in configuration");
            string schema = AbstractChatCompletionClient.GetSchemaFor(schemaOrSampleObject);
            using var client = new AbstractChatCompletionClient(new Uri(url), model, apikey,
                schema);

            (var result, var usage) = await client.CompleteAsync2(
                context,
                cfg.SystemPrompt,
                cfg.UserPrompt,
                token.Token
            );

            await using var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream());
            writer.WriteStartObject();
            writer.WritePropertyName("Result");
            writer.WriteObject(result);
            writer.WriteComma();
            writer.WritePropertyName("Usage");
            usage.Write(writer);
            writer.WriteEndObject();
        }
    }

    private AiConnectionString GetAiConnectionString(string name)
    {
        using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext serverCtx))
        using (serverCtx.OpenReadTransaction())
        {
            return ServerStore.Cluster.ReadRawDatabaseRecord(serverCtx, DatabaseName).GetAiConnectionString(name) 
                 ?? throw new InvalidOperationException("Cannot find connection string: " + name);
        }
    }
}
