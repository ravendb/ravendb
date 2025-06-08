using System.Threading.Tasks;
using Raven.Server.Documents.AI.GenAi;
using Raven.Server.Documents.ETL.Providers.AI.GenAi.Handlers.Processors;
using Raven.Server.Routing;
using Sparrow.Json;

namespace Raven.Server.Documents.ETL.Providers.AI.GenAi.Handlers;

public sealed class GenAiHandler : DatabaseRequestHandler
{
    [RavenAction("/databases/*/admin/ai/gen-ai/test", "POST", AuthorizationStatus.DatabaseAdmin)]
    public async Task PostScriptTest()
    {
        using (var processor = new GenAiHandlerProcessorForPostScriptTest(this))
            await processor.ExecuteAsync();
    }

    [RavenAction("/databases/*/admin/ai/gen-ai/to-json-schema", "POST", AuthorizationStatus.DatabaseAdmin)]
    public async Task GetJsonSchemaFromSampleObject()
    {
        using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
        {
            var sampleObj = await context.ReadForMemoryAsync(RequestBodyStream(), "etl/toJsonSchema");

            await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                var schema = OllamaChatCompletionClient.GetSchemaFor(sampleObj.ToString());

                writer.WriteStartObject();
                writer.WritePropertyName("Result");
                writer.WriteString(schema);
                writer.WriteEndObject();
            }
        }
    }
}
