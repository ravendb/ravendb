using System;
using System.Collections.Generic;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.ETL.Queue;

public class QueueConnectionString : ConnectionString
{
    public QueueProvider Provider { get; set; }

    public KafkaConnectionSettings KafkaConnectionSettings { get; set; }

    public RabbitMqConnectionSettings RabbitMqConnectionSettings { get; set; }
    
    public override ConnectionStringType Type => ConnectionStringType.Queue;

    protected override void ValidateImpl(ref List<string> errors)
    {
        switch (Provider)
        {
            case QueueProvider.Kafka:
                if (KafkaConnectionSettings == null || string.IsNullOrWhiteSpace(KafkaConnectionSettings.Url))
                {
                    errors.Add($"{nameof(KafkaConnectionSettings)} has no valid setting.");
                }
                break;
            case QueueProvider.RabbitMq:
                if (RabbitMqConnectionSettings == null || string.IsNullOrWhiteSpace(RabbitMqConnectionSettings.ConnectionString))
                {
                    errors.Add($"{nameof(RabbitMqConnectionSettings)} has no valid setting.");
                }
                break;
            default:
                throw new NotSupportedException($"'{Provider}' is not supported");
        }
    }

    public string GetUrl()
    {
        string url;

        switch (Provider)
        {
            case QueueProvider.Kafka:
                url = KafkaConnectionSettings.Url;
                break;
            case QueueProvider.RabbitMq:
                var connectionString = RabbitMqConnectionSettings.ConnectionString;

                int indexOfStartServerUri = connectionString.IndexOf("@", StringComparison.OrdinalIgnoreCase);

                url = indexOfStartServerUri != -1 ? connectionString.Substring(indexOfStartServerUri + 1) : null;
                break;
            default:
                throw new NotSupportedException($"'{Provider}' is not supported");
        }

        return url;
    }
    
    public override DynamicJsonValue ToJson()
    {
        DynamicJsonValue json = base.ToJson();
        json[nameof(Provider)] = Provider;
        json[nameof(KafkaConnectionSettings)] = KafkaConnectionSettings?.ToJson();

        return json;
    }
}
