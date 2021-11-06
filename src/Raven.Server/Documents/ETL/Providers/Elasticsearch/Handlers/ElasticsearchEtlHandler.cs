using System.Threading.Tasks;
using Raven.Server.Documents.ETL.Providers.ElasticSearch.Test;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.ETL.Providers.ElasticSearch.Handlers
{
    public class ElasticSearchEtlHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/admin/etl/elasticsearch/test", "POST", AuthorizationStatus.DatabaseAdmin)]
        public async Task PostScriptTest()
        {
            using (Database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                var dbDoc = await context.ReadForMemoryAsync(RequestBodyStream(), "TestElasticSearchEtlScript");
                var testScript = JsonDeserializationServer.TestElasticSearchEtlScript(dbDoc);

                using (ElasticSearchEtl.TestScript(testScript, Database, ServerStore, context, out var result))
                {
                    await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                    {
                        var djv = (DynamicJsonValue)TypeConverter.ToBlittableSupportedType(result);
                        writer.WriteObject(context.ReadObject(djv, "etl/elasticsearch/test"));
                    }
                }
            }
        }
    }
}
