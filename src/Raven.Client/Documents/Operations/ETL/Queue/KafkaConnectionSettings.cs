using System;
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

    private bool Equals(KafkaConnectionSettings other)
    {
        return string.Equals(BootstrapServers, other.BootstrapServers, StringComparison.OrdinalIgnoreCase) && Equals(ConnectionOptions, other.ConnectionOptions) && UseRavenCertificate == other.UseRavenCertificate;
    }

    public override bool Equals(object obj)
    {
        return ReferenceEquals(this, obj) || obj is KafkaConnectionSettings other && Equals(other);
    }

    public override int GetHashCode()
    {
        var hashCode = new HashCode();
        hashCode.Add(BootstrapServers);
        hashCode.Add(ConnectionOptions);
        hashCode.Add(UseRavenCertificate);
        return hashCode.ToHashCode();
    }
}
