using System.Threading.Tasks;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.QueueSink.Handlers;

public class QueueSinkHandler : DatabaseRequestHandler
{
    [RavenAction("/databases/*/admin/queue-sink/test", "POST", AuthorizationStatus.DatabaseAdmin)]
    public async Task PostScriptTest()
    {
        using (Database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
        {
            var dbDoc = await context.ReadForMemoryAsync(RequestBodyStream(), "TestQueueSinkScript");
            var testScript = JsonDeserializationServer.TestQueueSinkScript(dbDoc);

            var result = QueueSinkProcess.TestScript(testScript, context, Database);

            await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                var djv = (DynamicJsonValue)TypeConverter.ToBlittableSupportedType(result);
                writer.WriteObject(context.ReadObject(djv, "queue-sink/test"));
            }
        }
    }
}
