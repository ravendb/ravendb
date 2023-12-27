using System;
using System.IO;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;
using Confluent.Kafka;
using Newtonsoft.Json;
using Raven.Client.Documents.Operations.ETL.Queue;
using Raven.Server.Routing;
using Raven.Server.Web.System;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.ETL.Providers.Queue.Handlers
{
    public class QueueEtlConnectionHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/admin/etl/queue/kafka/test-connection", "POST", AuthorizationStatus.DatabaseAdmin)]
        public async Task GetTestKafkaConnectionResult()
        {
            StringBuilder errorHandlerDetails = null;
            StringBuilder logDetails = null;

            try
            {
                string jsonConfig = await new StreamReader(HttpContext.Request.Body).ReadToEndAsync();
                var settings = JsonConvert.DeserializeObject<KafkaConnectionSettings>(jsonConfig);

                var adminConfig = new AdminClientConfig() { BootstrapServers = settings.BootstrapServers };

                QueueBrokerConnectionHelper.SetupKafkaClientConfig(adminConfig, settings, Database.ServerStore.Server.Certificate);

                using var adminClient = new AdminClientBuilder(adminConfig)
                    .SetErrorHandler((client, error) =>
                    {
                        errorHandlerDetails ??= new StringBuilder();

                        errorHandlerDetails.AppendLine(error.ToString());
                    })
                    .SetLogHandler((client, message) =>
                    {
                        logDetails ??= new StringBuilder();

                        logDetails.AppendLine($"{message.Facility} {message.Message}");
                    })
                    .Build();
                var metadata = adminClient.GetMetadata(TimeSpan.FromSeconds(10));

                foreach (var brokerMetadata in metadata.Brokers)
                {
                    var host = brokerMetadata.Host;
                    var port = brokerMetadata.Port;

                    using (var client = new TcpClient())
                    {
                        try
                        {
                            await client.ConnectAsync(host, port);
                            client.Close();
                        }
                        catch (SocketException ex)
                        {
                            throw new Exception($"Failed to connect to the broker {brokerMetadata.BrokerId}: {host}:{port}", ex);
                        }
                    }
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
                    var error = ex.ToString();

                    if (errorHandlerDetails is not null)
                        error += $"{Environment.NewLine}ERROR DETAILS:{Environment.NewLine}{errorHandlerDetails}";

                    if (logDetails is not null)
                        error += $"{Environment.NewLine}LOGS:{Environment.NewLine}{logDetails}";

                    await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                    {
                        context.Write(writer,
                            new DynamicJsonValue
                            {
                                [nameof(NodeConnectionTestResult.Success)] = false,
                                [nameof(NodeConnectionTestResult.Error)] = error
                            });
                    }
                }
            }
        }

        [RavenAction("/databases/*/admin/etl/queue/rabbitmq/test-connection", "POST", AuthorizationStatus.DatabaseAdmin)]
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
