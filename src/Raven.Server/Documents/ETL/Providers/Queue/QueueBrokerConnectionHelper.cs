using System;
using System.Collections.Generic;
using System.IO;
using System.Security.Cryptography;
using Confluent.Kafka;
using RabbitMQ.Client;
using Raven.Client.Documents.Operations.ETL.Queue;
using Raven.Server.Utils;
using Sparrow.Logging;

namespace Raven.Server.Documents.ETL.Providers.Queue;

public static class QueueBrokerConnectionHelper
{
    public static IProducer<string, byte[]> CreateKafkaProducer(KafkaConnectionSettings settings, string transactionalId, Logger logger, string etlProcessName,
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
                    logger.Operations($"ETL process: {etlProcessName}. {logMessage.Message} (level: {logMessage.Level}, facility: {logMessage.Facility}");
            })
            .Build();

        return producer;
    }

    public static void SetupKafkaClientConfig(ClientConfig config, KafkaConnectionSettings settings, CertificateUtils.CertificateHolder certificateHolder = null)
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
        return connectionFactory.CreateConnection();
    }
}
