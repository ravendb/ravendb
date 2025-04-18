using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.ETL.Queue;

public sealed class RabbitMqConnectionSettings
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

    private bool Equals(RabbitMqConnectionSettings other)
    {
        return ConnectionString == other.ConnectionString;
    }

    public override bool Equals(object obj)
    {
        return ReferenceEquals(this, obj) || obj is RabbitMqConnectionSettings other && Equals(other);
    }

    public override int GetHashCode()
    {
        return (ConnectionString != null ? ConnectionString.GetHashCode() : 0);
    }
}
