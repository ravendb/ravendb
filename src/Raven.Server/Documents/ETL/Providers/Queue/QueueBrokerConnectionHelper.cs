using System;
using System.Collections.Generic;
using System.IO;
using Amazon;
using Amazon.SQS;
using Azure.Identity;
using Azure.Messaging.ServiceBus;
using Azure.Storage.Queues;
using System.Security.Cryptography;
using Confluent.Kafka;
using RabbitMQ.Client;
using Raven.Client.Documents.Operations.ETL.Queue;
using Raven.Server.Utils;
using Sparrow.Server.Logging;

namespace Raven.Server.Documents.ETL.Providers.Queue;

public static class QueueBrokerConnectionHelper
{
    public static IProducer<string, byte[]> CreateKafkaProducer(KafkaConnectionSettings settings,
        string transactionalId, RavenLogger logger, string etlProcessName,
        CertificateUtils.CertificateHolder certificateHolder = null)
    {
        ProducerConfig config = new()
        {
            BootstrapServers = settings.BootstrapServers,
            TransactionalId = transactionalId,
            ClientId = transactionalId,
            EnableIdempotence = true
        };

        SetupKafkaClientConfig(config, settings, certificateHolder);

        IProducer<string, byte[]> producer = new ProducerBuilder<string, byte[]>(config)
            .SetErrorHandler((producer, error) =>
            {
                if (logger.IsErrorEnabled)
                    logger.Error(
                        $"ETL process '{etlProcessName}' got the following Kafka producer " +
                        $"{(error.IsFatal ? "fatal" : "non fatal")}{(error.IsBrokerError ? " broker" : string.Empty)} error: {error.Reason} " +
                        $"(code: {error.Code}, is local: {error.IsLocalError})");
            })
            .SetLogHandler((producer, logMessage) =>
            {
                if (logger.IsInfoEnabled)
                    logger.Info(
                        $"ETL process: {etlProcessName}. {logMessage.Message} (level: {logMessage.Level}, facility: {logMessage.Facility}");
            })
            .Build();

        return producer;
    }

    public static void SetupKafkaClientConfig(ClientConfig config, KafkaConnectionSettings settings,
        CertificateUtils.CertificateHolder certificateHolder = null)
    {
        if (settings.UseRavenCertificate && certificateHolder?.ClientCertificate != null)
        {
            config.SslCertificatePem = certificateHolder.ClientCertificate.ExportCertificatePem();
            config.SslKeyPem = (certificateHolder.PrivateKey as RSA).GetExportableRsaPrivateKey().ExportRSAPrivateKeyPem();
            config.SecurityProtocol = SecurityProtocol.Ssl;
        }

        if (settings.ConnectionOptions != null)
        {
            foreach (KeyValuePair<string, string> option in settings.ConnectionOptions)
            {
                config.Set(option.Key, option.Value);
            }
        }
    }

    public static IConnection CreateRabbitMqConnection(RabbitMqConnectionSettings settings)
    {
        var connectionFactory = new ConnectionFactory { Uri = new Uri(settings.ConnectionString) };
        return connectionFactory.CreateConnectionAsync().GetAwaiter().GetResult();
    }

    public static QueueClient CreateAzureQueueStorageClient(
        AzureQueueStorageConnectionSettings azureQueueStorageConnectionSettings, string queueName)
    {
        QueueClient queueClient = null;

        if (azureQueueStorageConnectionSettings.ConnectionString != null)
        {
            queueClient = new QueueClient(azureQueueStorageConnectionSettings.ConnectionString,
                queueName);
        }

        else if (azureQueueStorageConnectionSettings.EntraId != null)
        {
            var queueUri = new Uri($"{azureQueueStorageConnectionSettings.GetStorageUrl()}{queueName}");

            queueClient = new QueueClient(
                queueUri,
                new ClientSecretCredential(
                    azureQueueStorageConnectionSettings.EntraId.TenantId,
                    azureQueueStorageConnectionSettings.EntraId.ClientId,
                    azureQueueStorageConnectionSettings.EntraId.ClientSecret));
        }
        else if (azureQueueStorageConnectionSettings.Passwordless != null)
        {
            var queueUri = new Uri($"{azureQueueStorageConnectionSettings.GetStorageUrl()}{queueName}");
            queueClient = new QueueClient(queueUri, new DefaultAzureCredential());
        }

        return queueClient;
    }

    public static QueueServiceClient CreateAzureQueueStorageServiceClient(
        AzureQueueStorageConnectionSettings azureQueueStorageConnectionSettings)
    {
        QueueServiceClient queueServiceClient = null;

        if (azureQueueStorageConnectionSettings.ConnectionString != null)
        {
            queueServiceClient =
                new QueueServiceClient(azureQueueStorageConnectionSettings.ConnectionString);
        }

        else if (azureQueueStorageConnectionSettings.EntraId != null)
        {
            var queueUri = new Uri(azureQueueStorageConnectionSettings.GetStorageUrl());

            queueServiceClient = new QueueServiceClient(
                queueUri,
                new ClientSecretCredential(
                    azureQueueStorageConnectionSettings.EntraId.TenantId,
                    azureQueueStorageConnectionSettings.EntraId.ClientId,
                    azureQueueStorageConnectionSettings.EntraId.ClientSecret));
        }
        else if (azureQueueStorageConnectionSettings.Passwordless != null)
        {
            var queueUri = new Uri($"{azureQueueStorageConnectionSettings.GetStorageUrl()}");
            queueServiceClient = new QueueServiceClient(queueUri, new DefaultAzureCredential());
        }

        return queueServiceClient;
    }

    public static IAmazonSQS CreateAmazonSqsClient(AmazonSqsConnectionSettings connectionSettings)
    {
        AmazonSQSClient sqsClient = null;

        if (connectionSettings.Basic != null)
        {
            var region = RegionEndpoint.GetBySystemName(connectionSettings.Basic.RegionName);
            sqsClient = new AmazonSQSClient(connectionSettings.Basic.AccessKey, connectionSettings.Basic.SecretKey,
                region);
        }
        else if (connectionSettings.Passwordless)
        {
            sqsClient = new AmazonSQSClient();
        }
        else if (connectionSettings.UseEmulator)
        {
            var emulatorUrl = Environment.GetEnvironmentVariable(AmazonSqsConnectionSettings.EmulatorUrlEnvironmentVariable);
            if (string.IsNullOrEmpty(emulatorUrl))
            {
                throw new InvalidOperationException(
                    $"The environment variable '{AmazonSqsConnectionSettings.EmulatorUrlEnvironmentVariable}' is required when using the Amazon SQS emulator.");
            }

            sqsClient = new AmazonSQSClient("dummy-access-key", "dummy-secret-key",
                new AmazonSQSConfig { ServiceURL = emulatorUrl, UseHttp = true });
        }

        return sqsClient;
    }

    public static ServiceBusClient CreateAzureServiceBusClient(AzureServiceBusConnectionSettings azureServiceBusConnectionSettings)
    {
        if (string.IsNullOrWhiteSpace(azureServiceBusConnectionSettings.ConnectionString) == false)
        {
            return new ServiceBusClient(azureServiceBusConnectionSettings.ConnectionString);
        }

        if (azureServiceBusConnectionSettings.EntraId != null)
        {
            return new ServiceBusClient(
                azureServiceBusConnectionSettings.EntraId.Namespace,
                new ClientSecretCredential(
                    azureServiceBusConnectionSettings.EntraId.TenantId,
                    azureServiceBusConnectionSettings.EntraId.ClientId,
                    azureServiceBusConnectionSettings.EntraId.ClientSecret));
        }

        if (azureServiceBusConnectionSettings.Passwordless != null)
        {
            return new ServiceBusClient(
                azureServiceBusConnectionSettings.Passwordless.Namespace,
                new DefaultAzureCredential());
        }

        throw new InvalidOperationException("No valid Azure Service Bus connection settings provided.");
    }
}
