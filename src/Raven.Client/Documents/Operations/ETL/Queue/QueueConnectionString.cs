using System.Collections.Generic;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.ETL.Queue;

public class QueueConnectionString : ConnectionString
{
    public QueueProvider Provider { get; set; }

    public string Url { get; set; }

    public Dictionary<string, string> KafkaConnectionOptions { get; set; }

    public bool UseRavenCertificate { get; set; }

    public override ConnectionStringType Type => ConnectionStringType.Queue;

    protected override void ValidateImpl(ref List<string> errors)
    {
        if (string.IsNullOrEmpty(Url))
            errors.Add($"{nameof(Url)} cannot be empty");
    }

    public override bool IsEqual(ConnectionString connectionString)
    {
        if (connectionString is QueueConnectionString queueConnection) return base.IsEqual(connectionString) && Url == queueConnection.Url;

        return false;
    }

    public override DynamicJsonValue ToJson()
    {
        DynamicJsonValue json = base.ToJson();
        json[nameof(Provider)] = Provider;
        json[nameof(Url)] = Url;
        json[nameof(UseRavenCertificate)] = UseRavenCertificate;
        json[nameof(Name)] = Name;
        if (KafkaConnectionOptions != null)
        {
            json[nameof(KafkaConnectionOptions)] = DynamicJsonValue.Convert(KafkaConnectionOptions);
        }
        
        return json;
    }
}
