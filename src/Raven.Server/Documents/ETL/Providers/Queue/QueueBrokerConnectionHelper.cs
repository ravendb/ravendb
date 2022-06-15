using System;
using System.Collections.Generic;
using System.Security.Cryptography.X509Certificates;
using Confluent.Kafka;
using RabbitMQ.Client;
using Raven.Client.Documents.Operations.ETL.Queue;
using Sparrow.Logging;

namespace Raven.Server.Documents.ETL.Providers.Queue;

public static class QueueBrokerConnectionHelper
{
    public static IProducer<string, byte[]> CreateKafkaProducer(KafkaConnectionSettings settings, string transactionalId, Logger logger, string etlProcessName,
        X509Certificate2 certificate = null)
    {
        ProducerConfig config = new()
        {
            BootstrapServers = settings.BootstrapServers,
            TransactionalId = transactionalId,
            ClientId = transactionalId,
            EnableIdempotence = true
        };

        SetupKafkaClientConfig(config, settings, certificate);
        
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
                    logger.Operations($"ETL process: {etlProcessName}. {logMessage.Message} (level: {logMessage.Level}, facility: {logMessage.Facility}");
            })
            .Build();

        return producer;
    }

    public static void SetupKafkaClientConfig(ClientConfig config, KafkaConnectionSettings settings, X509Certificate2 certificate = null)
    {
        if (settings.UseRavenCertificate && certificate != null)
        {
            config.SslCertificatePem = Convert.ToBase64String(certificate.Export(X509ContentType.Cert));
            config.SslKeyPem = Convert.ToBase64String(certificate.Export(X509ContentType.Pkcs7));
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
        return connectionFactory.CreateConnection();
    }
}
