using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents.Conventions;
using Raven.Server.Documents.ETL.Providers.Raven.Test;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.ETL.Providers.Raven.Handlers
{
    public class RavenEtlHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/admin/etl/raven/test", "POST", AuthorizationStatus.DatabaseAdmin)]
        public async Task PostScriptTest()
        {
            using (Database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                var testConfig = await context.ReadForMemoryAsync(RequestBodyStream(), "TestRavenEtlScript");

                var testScript = JsonDeserializationServer.TestRavenEtlScript(testConfig);

                var result = (RavenEtlTestScriptResult) RavenEtl.TestScript(testScript, Database, ServerStore, context);

                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    var defaultConventions = new DocumentConventions();

                    var djv = new DynamicJsonValue()
                    {
                        [nameof(result.Commands)] = new DynamicJsonArray(result.Commands.Select(x => x.ToJson(defaultConventions, context))),
                        [nameof(result.TransformationErrors)] = new DynamicJsonArray(result.TransformationErrors.Select(x => x.ToJson())),
                        [nameof(result.DebugOutput)] = new DynamicJsonArray(result.DebugOutput)
                    };

                    writer.WriteObject(context.ReadObject(djv, "et/raven/test"));
                }
            }
        }
    }
}
