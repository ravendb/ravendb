using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.ETL.Queue;

public class RabbitMqConnectionSettings
{
    public string ConnectionString { get; set; }
    
    public DynamicJsonValue ToJson()
    {
        var json = new DynamicJsonValue
        {
            [nameof(ConnectionString)] = ConnectionString,
        };

        return json;
    }

    public DynamicJsonValue ToAuditJson()
    {
        return new DynamicJsonValue();
    }
}
