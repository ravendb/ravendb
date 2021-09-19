using System;
using System.IO;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Raven.Client.Documents.Operations.ETL.ElasticSearch;
using Raven.Server.Documents.ETL.Providers.ElasticSearch.Test;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Raven.Server.Web.System;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.ETL.Providers.ElasticSearch.Handlers
{
    public class ElasticSearchEtlHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/admin/etl/elasticsearch/test-connection", "POST", AuthorizationStatus.DatabaseAdmin)]
        public async Task GetTestSqlConnection()
        {
            try
            {
                var url = GetQueryStringValueAndAssertIfSingleAndNotEmpty("url");
                var authenticationJson = await new StreamReader(HttpContext.Request.Body).ReadToEndAsync();
                var authentication = JsonConvert.DeserializeObject<Authentication>(authenticationJson);

                var client = ElasticSearchHelper.CreateClient(new ElasticSearchConnectionString(){Nodes = new []{url}, Authentication = authentication});

                var pingResult = await client.PingAsync();

                if (pingResult.IsValid)
                {
                    DynamicJsonValue result = new DynamicJsonValue
                    {
                        [nameof(NodeConnectionTestResult.Success)] = true,
                    };

                    using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
                    await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                    {
                        context.Write(writer, result);
                    }    
                }
                
                using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
                {
                    await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                    {
                        context.Write(writer, new DynamicJsonValue
                        {
                            [nameof(NodeConnectionTestResult.Success)] = false,
                            [nameof(NodeConnectionTestResult.Error)] = pingResult.DebugInformation
                        });
                    }
                }
            }
            catch (Exception ex)
            {
                if (Logger.IsInfoEnabled)
                    Logger.Info("Error occurred during elasticsearch connection test", ex);

                using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
                {
                    await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                    {
                        context.Write(writer, new DynamicJsonValue
                        {
                            [nameof(NodeConnectionTestResult.Success)] = false,
                            [nameof(NodeConnectionTestResult.Error)] = ex.ToString()
                        });
                    }
                }
            }
        }
        
        [RavenAction("/databases/*/admin/etl/elasticsearch/test", "POST", AuthorizationStatus.Operator)]
        public async Task PostScriptTest()
        {
            using (Database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                var dbDoc = await context.ReadForMemoryAsync(RequestBodyStream(), "TestElasticSearchEtlScript");
                var testScript = JsonDeserializationServer.TestElasticSearchEtlScript(dbDoc);

                var result = (ElasticSearchEtlTestScriptResult)ElasticSearchEtl.TestScript(testScript, Database, ServerStore, context);

                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    var djv = (DynamicJsonValue)TypeConverter.ToBlittableSupportedType(result);
                    writer.WriteObject(context.ReadObject(djv, "etl/elasticsearch/test"));
                }
            }
        }
    }
}
