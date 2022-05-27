using System.Collections.Generic;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.ETL.Queue;

public class KafkaConnectionSettings
{
    public string Url { get; set; }
        
    public Dictionary<string, string> ConnectionOptions { get; set; }

    public bool UseRavenCertificate { get; set; }
    
    public DynamicJsonValue ToJson()
    {
        var json = new DynamicJsonValue
        {
            [nameof(Url)] = Url,
            [nameof(UseRavenCertificate)] = UseRavenCertificate
        };
        
        if (ConnectionOptions != null)
        {
            json[nameof(ConnectionOptions)] = DynamicJsonValue.Convert(ConnectionOptions);
        }

        return json;
    }
}
