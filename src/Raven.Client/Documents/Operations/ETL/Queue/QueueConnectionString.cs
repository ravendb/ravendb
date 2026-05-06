using System;
using System.Collections.Generic;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.ETL.Queue;

public sealed class QueueConnectionString : ConnectionString
{
    public QueueBrokerType BrokerType { get; set; }

    public KafkaConnectionSettings KafkaConnectionSettings { get; set; }

    public RabbitMqConnectionSettings RabbitMqConnectionSettings { get; set; }

    public AzureQueueStorageConnectionSettings AzureQueueStorageConnectionSettings { get; set; }
    
    public AmazonSqsConnectionSettings AmazonSqsConnectionSettings { get; set; }

    public AzureServiceBusConnectionSettings AzureServiceBusConnectionSettings { get; set; }

    public override ConnectionStringType Type => ConnectionStringType.Queue;

    protected override void ValidateImpl(List<string> errors)
    {
        switch (BrokerType)
        {
            case QueueBrokerType.Kafka:
                if (KafkaConnectionSettings == null || string.IsNullOrWhiteSpace(KafkaConnectionSettings.BootstrapServers))
                {
                    errors.Add($"{nameof(KafkaConnectionSettings)} has no valid setting.");
                }
                break;
            case QueueBrokerType.RabbitMq:
                if (RabbitMqConnectionSettings == null || string.IsNullOrWhiteSpace(RabbitMqConnectionSettings.ConnectionString))
                {
                    errors.Add($"{nameof(RabbitMqConnectionSettings)} has no valid setting.");
                }
                break;
            case QueueBrokerType.AzureQueueStorage:
                if (AzureQueueStorageConnectionSettings.IsValidConnection() == false)
                {
                    errors.Add($"{nameof(AzureQueueStorageConnectionSettings)} has no valid setting.");
                }
                break;
            case QueueBrokerType.AmazonSqs:
                if (AmazonSqsConnectionSettings.IsValidConnection() == false)
                {
                    errors.Add($"{nameof(AmazonSqsConnectionSettings)} has no valid setting.");
                }
                break;
            case QueueBrokerType.AzureServiceBus:
                if (AzureServiceBusConnectionSettings.IsValidConnection() == false)
                {
                    errors.Add($"{nameof(AzureServiceBusConnectionSettings)} has no valid setting.");
                }
                break;
            default:
                throw new NotSupportedException($"'{BrokerType}' broker is not supported");
        }
    }

    internal string GetUrl()
    {
        string url;

        switch (BrokerType)
        {
            case QueueBrokerType.Kafka:
                url = KafkaConnectionSettings.BootstrapServers;
                break;
            case QueueBrokerType.RabbitMq:
                var connectionString = RabbitMqConnectionSettings.ConnectionString;

                int indexOfStartServerUri = connectionString.IndexOf("@", StringComparison.OrdinalIgnoreCase);

                url = indexOfStartServerUri != -1 ? connectionString.Substring(indexOfStartServerUri + 1) : null;
                break;
            case QueueBrokerType.AzureQueueStorage:
                url = AzureQueueStorageConnectionSettings.GetStorageUrl();
                break;
            case QueueBrokerType.AmazonSqs:
                url = AmazonSqsConnectionSettings.GetQueueUrl();
                break;
            case QueueBrokerType.AzureServiceBus:
                url = AzureServiceBusConnectionSettings.GetServiceBusUrl();
                break;
            default:
                throw new NotSupportedException($"'{BrokerType}' broker is not supported");
        }

        return url;
    }
    
    public override DynamicJsonValue ToJson()
    {
        var json = base.ToJson();

        json[nameof(BrokerType)] = BrokerType;
        json[nameof(KafkaConnectionSettings)] = KafkaConnectionSettings?.ToJson();
        json[nameof(RabbitMqConnectionSettings)] = RabbitMqConnectionSettings?.ToJson();
        json[nameof(AzureQueueStorageConnectionSettings)] = AzureQueueStorageConnectionSettings?.ToJson();
        json[nameof(AmazonSqsConnectionSettings)] = AmazonSqsConnectionSettings?.ToJson();
        json[nameof(AzureServiceBusConnectionSettings)] = AzureServiceBusConnectionSettings?.ToJson();

        return json;
    }

    public override DynamicJsonValue ToAuditJson()
    {
        var json = base.ToAuditJson();

        json[nameof(BrokerType)] = BrokerType;
        json[nameof(KafkaConnectionSettings)] = KafkaConnectionSettings?.ToAuditJson();
        json[nameof(RabbitMqConnectionSettings)] = RabbitMqConnectionSettings?.ToAuditJson();
        json[nameof(AzureServiceBusConnectionSettings)] = AzureServiceBusConnectionSettings?.ToAuditJson();

        return json;
    }

    public override bool IsEqual(ConnectionString connectionString)
    {
        if (connectionString is QueueConnectionString queueConnectionString)
        {
            var isEqual = base.IsEqual(connectionString);
            if (isEqual == false)
                return false;

            if (BrokerType != queueConnectionString.BrokerType)
                return false;
            
            switch (BrokerType)
            {
                case QueueBrokerType.Kafka:
                    if (KafkaConnectionSettings == null && queueConnectionString.KafkaConnectionSettings == null)
                        return true;
            
                    if (KafkaConnectionSettings == null || queueConnectionString.KafkaConnectionSettings == null)
                        return false;
            
                    return KafkaConnectionSettings.Equals(queueConnectionString.KafkaConnectionSettings);
            
                case QueueBrokerType.RabbitMq:
                    if (RabbitMqConnectionSettings == null && queueConnectionString.RabbitMqConnectionSettings == null)
                        return true;
            
                    if (RabbitMqConnectionSettings == null || queueConnectionString.RabbitMqConnectionSettings == null)
                        return false;
            
                    return RabbitMqConnectionSettings.Equals(queueConnectionString.RabbitMqConnectionSettings);
            
                case QueueBrokerType.AzureQueueStorage:
                    if (AzureQueueStorageConnectionSettings == null && queueConnectionString.AzureQueueStorageConnectionSettings == null)
                        return true;
            
                    if (AzureQueueStorageConnectionSettings == null || queueConnectionString.AzureQueueStorageConnectionSettings == null)
                        return false;
            
                    return AzureQueueStorageConnectionSettings.Equals(queueConnectionString.AzureQueueStorageConnectionSettings);
                
                case QueueBrokerType.AmazonSqs:
                    if (AmazonSqsConnectionSettings == null && queueConnectionString.AmazonSqsConnectionSettings == null)
                        return true;

                    if (AmazonSqsConnectionSettings == null || queueConnectionString.AmazonSqsConnectionSettings == null)
                        return false;

                    return AmazonSqsConnectionSettings.Equals(queueConnectionString.AmazonSqsConnectionSettings);

                case QueueBrokerType.AzureServiceBus:
                    if (AzureServiceBusConnectionSettings == null && queueConnectionString.AzureServiceBusConnectionSettings == null)
                        return true;

                    if (AzureServiceBusConnectionSettings == null || queueConnectionString.AzureServiceBusConnectionSettings == null)
                        return false;

                    return AzureServiceBusConnectionSettings.Equals(queueConnectionString.AzureServiceBusConnectionSettings);

                default:
                    throw new NotSupportedException($"'{BrokerType}' broker is not supported");
            }
        }

        return false;
    }
}
