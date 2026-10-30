using System;
using System.Collections.Generic;
using Raven.Client.Extensions;
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
        return string.Equals(BootstrapServers, other.BootstrapServers, StringComparison.OrdinalIgnoreCase) && DictionaryExtensions.ContentEquals(ConnectionOptions, other.ConnectionOptions) && UseRavenCertificate == other.UseRavenCertificate;
    }

    public override bool Equals(object obj)
    {
        return ReferenceEquals(this, obj) || obj is KafkaConnectionSettings other && Equals(other);
    }

    public override int GetHashCode()
    {
        var optionsHash = 0;
        if (ConnectionOptions != null)
        {
            foreach (var option in ConnectionOptions)
                optionsHash += HashCode.Combine(option.Key, option.Value);
        }

        var hashCode = new HashCode();
        hashCode.Add(BootstrapServers, StringComparer.OrdinalIgnoreCase);
        hashCode.Add(optionsHash);
        hashCode.Add(UseRavenCertificate);
        return hashCode.ToHashCode();
    }
}
