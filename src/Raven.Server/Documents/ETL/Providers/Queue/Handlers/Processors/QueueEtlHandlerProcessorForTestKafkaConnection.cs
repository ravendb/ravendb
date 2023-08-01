using System;
using System.IO;
using System.Threading.Tasks;
using Confluent.Kafka;
using JetBrains.Annotations;
using Newtonsoft.Json;
using Raven.Client.Documents.Operations.ETL.Queue;
using Raven.Server.Documents.Handlers.Processors;
using Raven.Server.Web.System;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.ETL.Providers.Queue.Handlers.Processors
{
    internal sealed class QueueEtlHandlerProcessorForTestKafkaConnection<TRequestHandler, TOperationContext> : AbstractDatabaseHandlerProcessor<TRequestHandler, TOperationContext>
        where TOperationContext : JsonOperationContext
        where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
    {
        public QueueEtlHandlerProcessorForTestKafkaConnection([NotNull] TRequestHandler requestHandler) : base(requestHandler)
        {
        }

        public override async ValueTask ExecuteAsync()
        {
            try
            {
                string jsonConfig = await new StreamReader(HttpContext.Request.Body).ReadToEndAsync();
                var settings = JsonConvert.DeserializeObject<KafkaConnectionSettings>(jsonConfig);

                var adminConfig = new AdminClientConfig() { BootstrapServers = settings.BootstrapServers };

                QueueBrokerConnectionHelper.SetupKafkaClientConfig(adminConfig, settings, ServerStore.Server.Certificate);

                using var adminClient = new AdminClientBuilder(adminConfig).Build();
                adminClient.GetMetadata(TimeSpan.FromSeconds(10));

                DynamicJsonValue result = new()
                {
                    [nameof(NodeConnectionTestResult.Success)] = true,
                };

                using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
                await using (AsyncBlittableJsonTextWriter writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
                {
                    context.Write(writer, result);
                }
            }
            catch (Exception ex)
            {
                using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
                {
                    await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
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
}
