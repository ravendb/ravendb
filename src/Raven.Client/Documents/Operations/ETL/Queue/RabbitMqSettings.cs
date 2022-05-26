using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.ETL.Queue;

public class RabbitMqSettings
{
    public string Url { get; set; }
    
    public DynamicJsonValue ToJson()
    {
        var json = new DynamicJsonValue
        {
            [nameof(Url)] = Url,
        };

        return json;
    }
}
