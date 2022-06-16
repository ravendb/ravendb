using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Confluent.Kafka;
using Newtonsoft.Json;
using RabbitMQ.Client;
using Raven.Client.Documents.Operations.ETL.Queue;
using Raven.Server.Routing;
using Raven.Server.Web.System;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.ETL.Providers.Queue.Handlers
{
    public class QueueEtlConnectionHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/admin/etl/queue/test-connection/kafka", "POST", AuthorizationStatus.DatabaseAdmin)]
        public async Task GetTestKafkaConnectionResult()
        {
            try
            {
                string jsonConfig = await new StreamReader(HttpContext.Request.Body).ReadToEndAsync();
                var settings = JsonConvert.DeserializeObject<KafkaConnectionSettings>(jsonConfig);

                var adminConfig = new AdminClientConfig() { BootstrapServers = settings.BootstrapServers };

                QueueBrokerConnectionHelper.SetupKafkaClientConfig(adminConfig, settings, Database.ServerStore.Server.Certificate);

                using var adminClient = new AdminClientBuilder(adminConfig).Build();
                adminClient.GetMetadata(TimeSpan.FromSeconds(10));

                DynamicJsonValue result = new()
                {
                    [nameof(NodeConnectionTestResult.Success)] = true,
                };

                using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
                await using (AsyncBlittableJsonTextWriter writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    context.Write(writer, result);
                }
            }
            catch (Exception ex)
            {
                using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
                {
                    await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
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

        [RavenAction("/databases/*/admin/etl/queue/test-connection/rabbitmq", "POST", AuthorizationStatus.DatabaseAdmin)]
        public async Task GetTestRabbitMqConnectionResult()
        {
            try
            {
                string jsonConfig = await new StreamReader(HttpContext.Request.Body).ReadToEndAsync();
                var settings = JsonConvert.DeserializeObject<RabbitMqConnectionSettings>(jsonConfig);

                using (QueueBrokerConnectionHelper.CreateRabbitMqConnection(settings))
                {

                }

                DynamicJsonValue result = new()
                {
                    [nameof(NodeConnectionTestResult.Success)] = true,
                };

                using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
                await using (AsyncBlittableJsonTextWriter writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    context.Write(writer, result);
                }
            }
            catch (Exception ex)
            {
                using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
                {
                    await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
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
