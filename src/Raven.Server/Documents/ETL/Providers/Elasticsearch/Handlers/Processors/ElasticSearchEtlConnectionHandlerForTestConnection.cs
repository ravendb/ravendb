using System;
using System.IO;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using JetBrains.Annotations;
using Newtonsoft.Json;
using Raven.Client.Documents.Operations.ETL.ElasticSearch;
using Raven.Server.Documents.Handlers.Processors;
using Raven.Server.Web.System;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Authentication = Raven.Client.Documents.Operations.ETL.ElasticSearch.Authentication;

namespace Raven.Server.Documents.ETL.Providers.ElasticSearch.Handlers.Processors;

internal sealed class ElasticSearchEtlConnectionHandlerForTestConnection<TOperationContext> : AbstractDatabaseHandlerProcessor<TOperationContext>
    where TOperationContext : JsonOperationContext
{
    public ElasticSearchEtlConnectionHandlerForTestConnection([NotNull] AbstractDatabaseRequestHandler<TOperationContext> requestHandler)
        : base(requestHandler)
    {
    }

    public override async ValueTask ExecuteAsync()
    {
        try
        {
            string url = RequestHandler.GetQueryStringValueAndAssertIfSingleAndNotEmpty("url");
            string authenticationJson = await new StreamReader(HttpContext.Request.Body).ReadToEndAsync();
            Authentication authentication = JsonConvert.DeserializeObject<Authentication>(authenticationJson);

            ElasticsearchClient client = ElasticSearchHelper.CreateClient(new ElasticSearchConnectionString { Nodes = new[] { url }, Authentication = authentication });

            PingResponse pingResult = await client.PingAsync();

            if (pingResult.IsValidResponse)
            {
                DynamicJsonValue result = new() { [nameof(NodeConnectionTestResult.Success)] = true, [nameof(NodeConnectionTestResult.TcpServerUrl)] = url, };

                using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
                await using (AsyncBlittableJsonTextWriter writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
                {
                    context.Write(writer, result);
                }
            }
            else
            {
                using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
                {
                    await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
                    {
                        context.Write(writer, new DynamicJsonValue
                        {
                            [nameof(NodeConnectionTestResult.Success)] = false,
                            [nameof(NodeConnectionTestResult.Error)] = pingResult.DebugInformation
                        });
                    }
                }
            }
        }
        catch (Exception ex)
        {
            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
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
}
