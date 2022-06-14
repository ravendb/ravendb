using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Confluent.Kafka;
using Newtonsoft.Json;
using RabbitMQ.Client;
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
                string url = GetQueryStringValueAndAssertIfSingleAndNotEmpty("url");
                string jsonConfig = await new StreamReader(HttpContext.Request.Body).ReadToEndAsync();
                var config = JsonConvert.DeserializeObject<KafkaConnectionConfiguration>(jsonConfig);

                var adminConfig = new AdminClientConfig() { BootstrapServers = url };
                if (config != null && config.Configuration != null)
                {
                    foreach (KeyValuePair<string, string> option in config.Configuration)
                    {
                        adminConfig.Set(option.Key, option.Value);
                    }    
                }

                var adminClient = new AdminClientBuilder(adminConfig).Build();
                adminClient.GetMetadata(TimeSpan.FromSeconds(10));

                DynamicJsonValue result = new()
                {
                    [nameof(NodeConnectionTestResult.Success)] = true, [nameof(NodeConnectionTestResult.TcpServerUrl)] = url,
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
                string url = GetQueryStringValueAndAssertIfSingleAndNotEmpty("url");

                var connectionFactory = new ConnectionFactory() { Uri = new Uri(url) };
                using (connectionFactory.CreateConnection())
                {
                }

                DynamicJsonValue result = new()
                {
                    [nameof(NodeConnectionTestResult.Success)] = true, [nameof(NodeConnectionTestResult.TcpServerUrl)] = url,
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

public class KafkaConnectionConfiguration
{
    public Dictionary<string, string> Configuration { get; set; }
    public bool UseRavenCertificate { get; set; }
}
