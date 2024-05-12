using System;
using System.Collections.Generic;
using System.IO;
using Azure.Core.Pipeline;
using Azure.Identity;
using Azure.Storage.Queues;
using Azure.Storage.Queues.Models;
using Confluent.Kafka;
using NetTopologySuite.IO;
using Org.BouncyCastle.Utilities.IO.Pem;
using RabbitMQ.Client;
using Raven.Client.Documents.Operations.ETL.Queue;
using Raven.Server.Utils;
using Sparrow.Logging;
using PemWriter = Org.BouncyCastle.OpenSsl.PemWriter;

namespace Raven.Server.Documents.ETL.Providers.Queue;

public static class QueueBrokerConnectionHelper
{
    public static IProducer<string, byte[]> CreateKafkaProducer(KafkaConnectionSettings settings,
        string transactionalId, Logger logger, string etlProcessName,
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
                if (logger.IsOperationsEnabled)
                    logger.Operations(
                        $"ETL process '{etlProcessName}' got the following Kafka producer " +
                        $"{(error.IsFatal ? "fatal" : "non fatal")}{(error.IsBrokerError ? " broker" : string.Empty)} error: {error.Reason} " +
                        $"(code: {error.Code}, is local: {error.IsLocalError})");
            })
            .SetLogHandler((producer, logMessage) =>
            {
                if (logger.IsOperationsEnabled)
                    logger.Operations(
                        $"ETL process: {etlProcessName}. {logMessage.Message} (level: {logMessage.Level}, facility: {logMessage.Facility}");
            })
            .Build();

        return producer;
    }

    public static void SetupKafkaClientConfig(ClientConfig config, KafkaConnectionSettings settings,
        CertificateUtils.CertificateHolder certificateHolder = null)
    {
        if (settings.UseRavenCertificate && certificateHolder?.Certificate != null)
        {
            config.SslCertificatePem = ExportAsPem(new PemObject("CERTIFICATE", certificateHolder.Certificate.RawData));
            config.SslKeyPem = ExportAsPem(certificateHolder.PrivateKey.Key);
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

    private static string ExportAsPem(object @object)
    {
        using (var sw = new StringWriter())
        {
            var pemWriter = new PemWriter(sw);

            pemWriter.WriteObject(@object);

            return sw.ToString();
        }
    }

    public static IConnection CreateRabbitMqConnection(RabbitMqConnectionSettings settings)
    {
        var connectionFactory = new ConnectionFactory { Uri = new Uri(settings.ConnectionString) };
        return connectionFactory.CreateConnection();
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
        else if(azureQueueStorageConnectionSettings.Passwordless != null)
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
        else if(azureQueueStorageConnectionSettings.Passwordless != null)
        {
            var queueUri = new Uri($"{azureQueueStorageConnectionSettings.GetStorageUrl()}");
            queueServiceClient = new QueueServiceClient(queueUri, new DefaultAzureCredential());
        }

        return queueServiceClient;
    }
}
