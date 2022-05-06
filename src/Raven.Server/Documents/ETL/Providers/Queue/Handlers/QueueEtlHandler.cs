using System.Threading.Tasks;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.ETL.Providers.Queue.Handlers
{
    public class QueueEtlHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/admin/etl/queue/test", "POST", AuthorizationStatus.DatabaseAdmin)]
        public async Task PostScriptTest()
        {
            using (Database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                var dbDoc = await context.ReadForMemoryAsync(RequestBodyStream(), "TestQueueEtlScript");
                var testScript = JsonDeserializationServer.TestQueueEtlScript(dbDoc);

                using (QueueEtl.TestScript(testScript, Database, ServerStore, context, out var result))
                {
                    await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                    {
                        var djv = (DynamicJsonValue)TypeConverter.ToBlittableSupportedType(result);
                        writer.WriteObject(context.ReadObject(djv, "etl/queue/test"));
                    }
                }
            }
        }
    }
}
