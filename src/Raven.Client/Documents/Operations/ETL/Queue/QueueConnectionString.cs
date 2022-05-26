using System.Collections.Generic;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.ETL.Queue;

public class QueueConnectionString : ConnectionString
{
    public QueueProvider Provider { get; set; }

    public KafkaSettings KafkaSettings { get; set; }

    public RabbitMqSettings RabbitMqSettings { get; set; }
    
    public override ConnectionStringType Type => ConnectionStringType.Queue;

    protected override void ValidateImpl(ref List<string> errors)
    {
        if (Provider == QueueProvider.Kafka)
        {
            if (KafkaSettings == null || string.IsNullOrWhiteSpace(KafkaSettings.Url))
            {
                errors.Add($"{nameof(KafkaSettings)} has no valid setting.");
            }
        }
        else if (Provider == QueueProvider.RabbitMq)
        {
            if (RabbitMqSettings == null || string.IsNullOrWhiteSpace(RabbitMqSettings.Url))
            {
                errors.Add($"{nameof(RabbitMqSettings)} has no valid setting.");
            }
        }
    }

    public string GetUrl()
    {
        var url = "";
            
        if (Provider == QueueProvider.Kafka)
        {
            url = KafkaSettings.Url;
        }
        else if (Provider == QueueProvider.RabbitMq)
        {
            url = RabbitMqSettings.Url;
        }

        return url;
    }
    
    public override DynamicJsonValue ToJson()
    {
        DynamicJsonValue json = base.ToJson();
        json[nameof(Provider)] = Provider;
        json[nameof(KafkaSettings)] = KafkaSettings?.ToJson();

        return json;
    }
}
