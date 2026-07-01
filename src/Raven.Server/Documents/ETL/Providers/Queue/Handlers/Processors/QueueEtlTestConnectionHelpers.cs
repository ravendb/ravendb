using System;
using System.IO;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;
using Amazon.SQS;
using Azure.Messaging.ServiceBus;
using Azure.Storage.Queues;
using Confluent.Kafka;
using Newtonsoft.Json;
using Raven.Client.Documents.Operations.ETL.Queue;
using Raven.Server.Web;
using Raven.Server.Web.System;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.ETL.Providers.Queue.Handlers.Processors;

internal static class QueueEtlTestConnectionHelpers
{
    public static async Task TestKafkaAsync(RequestHandler requestHandler)
    {
        StringBuilder errorHandlerDetails = null;
        StringBuilder logDetails = null;

        try
        {
            string jsonConfig = await new StreamReader(requestHandler.HttpContext.Request.Body).ReadToEndAsync();
            var settings = JsonConvert.DeserializeObject<KafkaConnectionSettings>(jsonConfig);

            var adminConfig = new AdminClientConfig() { BootstrapServers = settings.BootstrapServers };

            QueueBrokerConnectionHelper.SetupKafkaClientConfig(adminConfig, settings, requestHandler.ServerStore.Server.Certificate);

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

            await WriteSuccessAsync(requestHandler);
        }
        catch (Exception ex)
        {
            using (requestHandler.ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                var error = ex.ToString();

                if (errorHandlerDetails is not null)
                    error += $"{Environment.NewLine}ERROR DETAILS:{Environment.NewLine}{errorHandlerDetails}";

                if (logDetails is not null)
                    error += $"{Environment.NewLine}LOGS:{Environment.NewLine}{logDetails}";

                await using (var writer = new AsyncBlittableJsonTextWriter(context, requestHandler.ResponseBodyStream()))
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

    public static async Task TestRabbitMqAsync(RequestHandler requestHandler)
    {
        try
        {
            string jsonConfig = await new StreamReader(requestHandler.HttpContext.Request.Body).ReadToEndAsync();
            var settings = JsonConvert.DeserializeObject<RabbitMqConnectionSettings>(jsonConfig);

            using (QueueBrokerConnectionHelper.CreateRabbitMqConnection(settings))
            {
            }

            await WriteSuccessAsync(requestHandler);
        }
        catch (Exception ex)
        {
            await WriteErrorAsync(requestHandler, ex);
        }
    }

    public static async Task TestAzureQueueStorageAsync(RequestHandler requestHandler)
    {
        try
        {
            string authenticationJson = await new StreamReader(requestHandler.HttpContext.Request.Body).ReadToEndAsync();
            AzureQueueStorageConnectionSettings connectionSettings = JsonConvert.DeserializeObject<AzureQueueStorageConnectionSettings>(authenticationJson);

            QueueServiceClient client = QueueBrokerConnectionHelper.CreateAzureQueueStorageServiceClient(connectionSettings);

            await client.GetPropertiesAsync();

            await WriteSuccessAsync(requestHandler);
        }
        catch (Exception ex)
        {
            await WriteErrorAsync(requestHandler, ex);
        }
    }

    public static async Task TestAmazonSqsAsync(RequestHandler requestHandler)
    {
        try
        {
            string authenticationJson = await new StreamReader(requestHandler.HttpContext.Request.Body).ReadToEndAsync();
            AmazonSqsConnectionSettings connectionSettings = JsonConvert.DeserializeObject<AmazonSqsConnectionSettings>(authenticationJson);

            IAmazonSQS client = QueueBrokerConnectionHelper.CreateAmazonSqsClient(connectionSettings);

            try
            {
                // Attempt to get the queue URL, which will validate the credentials
                await client.GetQueueUrlAsync("connection-test");

                // If we successfully get the queue URL, the credentials and permissions are valid
                await WriteSuccessAsync(requestHandler);
            }
            catch (AmazonSQSException ex) when (ex.ErrorCode.Contains("NonExistentQueue"))
            {
                // In this case, it means the connection is valid but the queue is not accessible
                await WriteSuccessAsync(requestHandler);
            }
        }
        catch (Exception ex)
        {
            await WriteErrorAsync(requestHandler, ex);
        }
    }

    public static async Task TestAzureServiceBusAsync(RequestHandler requestHandler)
    {
        try
        {
            string authenticationJson = await new StreamReader(requestHandler.HttpContext.Request.Body).ReadToEndAsync();
            AzureServiceBusConnectionSettings connectionSettings = JsonConvert.DeserializeObject<AzureServiceBusConnectionSettings>(authenticationJson);

            var probeEntity = $"ravendb-connection-test-{Guid.NewGuid():N}";
            await using var client = QueueBrokerConnectionHelper.CreateAzureServiceBusClient("RavenDB-test-connectivity", connectionSettings);
            await using var receiver = client.CreateReceiver(probeEntity);

            try
            {
                await receiver.PeekMessageAsync();
            }
            catch (ServiceBusException ex) when (ex.Reason == ServiceBusFailureReason.MessagingEntityNotFound)
            {
                // Credentials and network are valid; the probe entity not existing is expected.
            }

            await WriteSuccessAsync(requestHandler);
        }
        catch (Exception ex)
        {
            await WriteErrorAsync(requestHandler, ex);
        }
    }

    private static async Task WriteSuccessAsync(RequestHandler requestHandler)
    {
        DynamicJsonValue result = new() { [nameof(NodeConnectionTestResult.Success)] = true };
        using (requestHandler.ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
        await using (var writer = new AsyncBlittableJsonTextWriter(context, requestHandler.ResponseBodyStream()))
        {
            context.Write(writer, result);
        }
    }

    private static async Task WriteErrorAsync(RequestHandler requestHandler, Exception exception)
    {
        var errorJson = JsonConvert.SerializeObject(new { Message = exception.Message, Error = exception.ToString() });

        using (requestHandler.ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
        await using (var writer = new AsyncBlittableJsonTextWriter(context, requestHandler.ResponseBodyStream()))
        {
            context.Write(writer,
                new DynamicJsonValue
                {
                    [nameof(NodeConnectionTestResult.Success)] = false,
                    [nameof(NodeConnectionTestResult.Error)] = errorJson
                });
        }
    }
}
