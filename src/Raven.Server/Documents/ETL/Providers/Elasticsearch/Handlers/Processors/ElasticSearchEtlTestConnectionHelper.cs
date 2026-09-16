using System;
using System.IO;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Newtonsoft.Json;
using Raven.Client.Documents.Operations.ETL.ElasticSearch;
using Raven.Server.Web;
using Raven.Server.Web.System;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Authentication = Raven.Client.Documents.Operations.ETL.ElasticSearch.Authentication;

namespace Raven.Server.Documents.ETL.Providers.ElasticSearch.Handlers.Processors;

internal static class ElasticSearchEtlTestConnectionHelper
{
    public static async Task ExecuteAsync(RequestHandler requestHandler)
    {
        try
        {
            string url = requestHandler.GetQueryStringValueAndAssertIfSingleAndNotEmpty("url");
            string authenticationJson = await new StreamReader(requestHandler.HttpContext.Request.Body).ReadToEndAsync();
            Authentication authentication = JsonConvert.DeserializeObject<Authentication>(authenticationJson);

            ElasticsearchClient client = ElasticSearchHelper.CreateClient(new ElasticSearchConnectionString { Nodes = new[] { url }, Authentication = authentication });

            PingResponse pingResult = await client.PingAsync();

            if (pingResult.IsValidResponse)
            {
                DynamicJsonValue result = new() { [nameof(NodeConnectionTestResult.Success)] = true, [nameof(NodeConnectionTestResult.TcpServerUrl)] = url, };

                using (requestHandler.ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
                await using (var writer = new AsyncBlittableJsonTextWriter(context, requestHandler.ResponseBodyStream()))
                {
                    context.Write(writer, result);
                }
            }
            else
            {
                using (requestHandler.ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
                await using (var writer = new AsyncBlittableJsonTextWriter(context, requestHandler.ResponseBodyStream()))
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
            using (requestHandler.ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriter(context, requestHandler.ResponseBodyStream()))
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
