using System;
using System.IO;
using System.Threading.Tasks;
using Azure.Storage.Queues;
using JetBrains.Annotations;
using Newtonsoft.Json;
using Raven.Client.Documents.Operations.ETL.Queue;
using Raven.Server.Documents.Handlers.Processors;
using Raven.Server.Web.System;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.ETL.Providers.Queue.Handlers.Processors;

internal sealed class
    QueueEtlHandlerProcessorForTestAzureQueueStorageConnection<TRequestHandler, TOperationContext> :
    AbstractDatabaseHandlerProcessor<TRequestHandler, TOperationContext>
    where TOperationContext : JsonOperationContext
    where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
{
    public QueueEtlHandlerProcessorForTestAzureQueueStorageConnection([NotNull] TRequestHandler requestHandler) :
        base(requestHandler)
    {
    }

    public override async ValueTask ExecuteAsync()
    {
        try
        {
            string authenticationJson = await new StreamReader(HttpContext.Request.Body).ReadToEndAsync();
            AzureQueueStorageConnectionSettings connectionSettings =
                JsonConvert.DeserializeObject<AzureQueueStorageConnectionSettings>(authenticationJson);

            QueueServiceClient client =
                QueueBrokerConnectionHelper.CreateAzureQueueStorageServiceClient(connectionSettings);

            await client.GetPropertiesAsync();

            DynamicJsonValue result = new() { [nameof(NodeConnectionTestResult.Success)] = true, };
            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            await using (AsyncBlittableJsonTextWriter writer = new(context, RequestHandler.ResponseBodyStream()))
            {
                context.Write(writer, result);
            }
        }
        catch (Exception ex)
        {
            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                await using (var writer =
                             new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
                {
                    context.Write(writer,
                        new DynamicJsonValue
                        {
                            [nameof(NodeConnectionTestResult.Success)] = false,
                            [nameof(NodeConnectionTestResult.Error)] = ex.ToString()
                        });
                }
            }
        }
    }
}
