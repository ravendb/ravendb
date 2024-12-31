using System;
using System.IO;
using System.Threading.Tasks;
using Amazon.SQS;
using JetBrains.Annotations;
using Newtonsoft.Json;
using Raven.Client.Documents.Operations.ETL.Queue;
using Raven.Server.Documents.Handlers.Processors;
using Raven.Server.Web.System;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.ETL.Providers.Queue.Handlers.Processors;

internal sealed class
    QueueEtlHandlerProcessorForTestAmazonSqsConnection<TRequestHandler, TOperationContext> :
    AbstractDatabaseHandlerProcessor<TRequestHandler, TOperationContext>
    where TOperationContext : JsonOperationContext
    where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
{
    public QueueEtlHandlerProcessorForTestAmazonSqsConnection([NotNull] TRequestHandler requestHandler) :
        base(requestHandler)
    {
    }

    public override async ValueTask ExecuteAsync()
    {
        try
        {
            string authenticationJson = await new StreamReader(HttpContext.Request.Body).ReadToEndAsync();
            AmazonSqsConnectionSettings connectionSettings =
                JsonConvert.DeserializeObject<AmazonSqsConnectionSettings>(authenticationJson);

            IAmazonSQS client = QueueBrokerConnectionHelper.CreateAmazonSqsClient(connectionSettings);

            try
            {
                // Attempt to get the queue URL, which will validate the credentials
                await client.GetQueueUrlAsync("connection-test");

                // If we successfully get the queue URL, the credentials and permissions are valid
                DynamicJsonValue result = new() { [nameof(NodeConnectionTestResult.Success)] = true };
                using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
                await using (AsyncBlittableJsonTextWriter writer = new(context, RequestHandler.ResponseBodyStream()))
                {
                    context.Write(writer, result);
                }
            }
            catch (AmazonSQSException ex) when (ex.ErrorCode.Contains("NonExistentQueue"))
            {
                // In this case, it means the connection is valid but the queue is not accessible
                DynamicJsonValue result = new() { [nameof(NodeConnectionTestResult.Success)] = true };
                using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
                await using (AsyncBlittableJsonTextWriter writer = new(context, RequestHandler.ResponseBodyStream()))
                {
                    context.Write(writer, result);
                }
            }
        }
        catch (Exception ex)
        {
            await WriteErrorResponse(ex.ToString());
        }
    }

    private async Task WriteErrorResponse(string errorMessage)
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
                        [nameof(NodeConnectionTestResult.Error)] = errorMessage
                    });
            }
        }
    }
}
