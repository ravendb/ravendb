using System.Collections.Generic;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.ETL.Queue;

public sealed class KafkaConnectionSettings
{
    public string BootstrapServers { get; set; }
        
    public Dictionary<string, string> ConnectionOptions { get; set; }

    public bool UseRavenCertificate { get; set; }
    
    public DynamicJsonValue ToJson()
    {
        var json = new DynamicJsonValue
        {
            [nameof(BootstrapServers)] = BootstrapServers,
            [nameof(UseRavenCertificate)] = UseRavenCertificate
        };
        
        if (ConnectionOptions != null)
        {
            json[nameof(ConnectionOptions)] = DynamicJsonValue.Convert(ConnectionOptions);
        }

        return json;
    }
    
    public DynamicJsonValue ToAuditJson()
    {
        var json = new DynamicJsonValue
        {
            [nameof(BootstrapServers)] = BootstrapServers,
            [nameof(UseRavenCertificate)] = UseRavenCertificate
        };
        
        return json;
    }
}
