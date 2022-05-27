using System;
using System.Collections.Generic;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.ETL.Queue;

public class QueueConnectionString : ConnectionString
{
    public QueueBroker BrokerType { get; set; }

    public KafkaConnectionSettings KafkaConnectionSettings { get; set; }

    public RabbitMqConnectionSettings RabbitMqConnectionSettings { get; set; }
    
    public override ConnectionStringType Type => ConnectionStringType.Queue;

    protected override void ValidateImpl(ref List<string> errors)
    {
        switch (BrokerType)
        {
            case QueueBroker.Kafka:
                if (KafkaConnectionSettings == null || string.IsNullOrWhiteSpace(KafkaConnectionSettings.Url))
                {
                    errors.Add($"{nameof(KafkaConnectionSettings)} has no valid setting.");
                }
                break;
            case QueueBroker.RabbitMq:
                if (RabbitMqConnectionSettings == null || string.IsNullOrWhiteSpace(RabbitMqConnectionSettings.ConnectionString))
                {
                    errors.Add($"{nameof(RabbitMqConnectionSettings)} has no valid setting.");
                }
                break;
            default:
                throw new NotSupportedException($"'{BrokerType}' broker is not supported");
        }
    }

    public string GetUrl()
    {
        string url;

        switch (BrokerType)
        {
            case QueueBroker.Kafka:
                url = KafkaConnectionSettings.Url;
                break;
            case QueueBroker.RabbitMq:
                var connectionString = RabbitMqConnectionSettings.ConnectionString;

                int indexOfStartServerUri = connectionString.IndexOf("@", StringComparison.OrdinalIgnoreCase);

                url = indexOfStartServerUri != -1 ? connectionString.Substring(indexOfStartServerUri + 1) : null;
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

        return json;
    }
}
