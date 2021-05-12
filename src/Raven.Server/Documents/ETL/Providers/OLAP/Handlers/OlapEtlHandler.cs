using System.Threading.Tasks;
using Raven.Server.Documents.ETL.Providers.OLAP.Test;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.ETL.Providers.OLAP.Handlers
{
    public class OlapEtlHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/admin/etl/olap/test", "POST", AuthorizationStatus.DatabaseAdmin)]
        public async Task PostScriptTest()
        {
            using (Database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                var testConfig = await context.ReadForMemoryAsync(RequestBodyStream(), "ola-etl-test");

                var testScript = JsonDeserializationServer.TestOlapEtlScript(testConfig);

                var result = (OlapEtlTestScriptResult)OlapEtl.TestScript(testScript, Database, ServerStore, context);

                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    var djv = (DynamicJsonValue)TypeConverter.ToBlittableSupportedType(result);
                    writer.WriteObject(context.ReadObject(djv, "olap-etl-test"));
                }
            }
        }
    }
}
