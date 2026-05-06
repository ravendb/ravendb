using System;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.ETL.Queue;

public sealed class AzureServiceBusConnectionSettings
{
    public string ConnectionString { get; set; }

    public AzureServiceBusEntraId EntraId { get; set; }

    public AzureServiceBusPasswordless Passwordless { get; set; }

    public bool IsValidConnection()
    {
        if (IsOnlyOneConnectionProvided() == false)
        {
            return false;
        }

        if (EntraId != null && EntraId.IsValid())
        {
            return true;
        }

        if (Passwordless != null && Passwordless.IsValid())
        {
            return true;
        }

        return TryExtractEndpoint(out _);
    }

    private bool IsOnlyOneConnectionProvided()
    {
        int count = 0;

        if (EntraId != null)
            count++;

        if (string.IsNullOrWhiteSpace(ConnectionString) == false)
            count++;

        if (Passwordless != null)
            count++;

        return count == 1;
    }

    public string GetServiceBusUrl()
    {
        if (string.IsNullOrWhiteSpace(ConnectionString) == false)
        {
            if (TryExtractEndpoint(out var endpoint))
                return endpoint;

            throw new InvalidOperationException("No endpoint provided");
        }

        return $"sb://{GetNamespace()}/";
    }

    private bool TryExtractEndpoint(out string endpoint)
    {
        endpoint = null;

        if (string.IsNullOrEmpty(ConnectionString))
            return false;

        var start = ConnectionString.IndexOf("sb://", StringComparison.OrdinalIgnoreCase);
        if (start < 0)
            return false;

        var end = ConnectionString.IndexOf(';', start);
        if (end < 0)
        {
            endpoint = ConnectionString.Substring(start);
            return true;
        }
       
        endpoint = ConnectionString.Substring(start, end - start);
        return true;
    }

    private string GetNamespace()
    {
        if (EntraId != null)
        {
            return EntraId.Namespace;
        }
        
        if (Passwordless != null)
        {
            return Passwordless.Namespace;
        }

        throw new InvalidOperationException("No namespace provided");
    }

    public DynamicJsonValue ToJson()
    {
        var json = new DynamicJsonValue();

        if (string.IsNullOrEmpty(ConnectionString) == false)
        {
            json[nameof(ConnectionString)] = ConnectionString;
        }

        if (EntraId != null)
        {
            json[nameof(EntraId)] = new DynamicJsonValue
            {
                [nameof(EntraId.Namespace)] = EntraId.Namespace,
                [nameof(EntraId.TenantId)] = EntraId.TenantId,
                [nameof(EntraId.ClientId)] = EntraId.ClientId,
                [nameof(EntraId.ClientSecret)] = EntraId.ClientSecret
            };
        }

        if (Passwordless != null)
        {
            json[nameof(Passwordless)] = new DynamicJsonValue
            {
                [nameof(Passwordless.Namespace)] = Passwordless.Namespace
            };
        }

        return json;
    }

    public DynamicJsonValue ToAuditJson()
    {
        var json = new DynamicJsonValue();

        if (string.IsNullOrEmpty(ConnectionString) == false)
        {
            json[nameof(ConnectionString)] = "<Contains-Secrets>";
        }

        if (EntraId != null)
        {
            json[nameof(EntraId)] = new DynamicJsonValue
            {
                [nameof(EntraId.Namespace)] = EntraId.Namespace,
                [nameof(EntraId.TenantId)] = EntraId.TenantId,
                [nameof(EntraId.ClientId)] = EntraId.ClientId,
            };
        }

        if (Passwordless != null)
        {
            json[nameof(Passwordless)] = new DynamicJsonValue
            {
                [nameof(Passwordless.Namespace)] = Passwordless.Namespace
            };
        }

        return json;
    }

    private bool Equals(AzureServiceBusConnectionSettings other)
    {
        return Equals(EntraId, other.EntraId) && ConnectionString == other.ConnectionString && Equals(Passwordless, other.Passwordless);
    }

    public override bool Equals(object obj)
    {
        return ReferenceEquals(this, obj) || obj is AzureServiceBusConnectionSettings other && Equals(other);
    }

    public override int GetHashCode()
    {
        return HashCode.Combine(EntraId, ConnectionString, Passwordless);
    }
}

public sealed class AzureServiceBusEntraId
{
    public string Namespace { get; set; }
    public string TenantId { get; set; }
    public string ClientId { get; set; }
    public string ClientSecret { get; set; }

    public bool IsValid()
    {
        return string.IsNullOrWhiteSpace(Namespace) == false &&
               string.IsNullOrWhiteSpace(TenantId) == false &&
               string.IsNullOrWhiteSpace(ClientId) == false &&
               string.IsNullOrWhiteSpace(ClientSecret) == false;
    }

    private bool Equals(AzureServiceBusEntraId other)
    {
        return Namespace == other.Namespace && TenantId == other.TenantId && ClientId == other.ClientId && ClientSecret == other.ClientSecret;
    }

    public override bool Equals(object obj)
    {
        return ReferenceEquals(this, obj) || obj is AzureServiceBusEntraId other && Equals(other);
    }

    public override int GetHashCode()
    {
        return HashCode.Combine(Namespace, TenantId, ClientId, ClientSecret);
    }
}

// this is used for machine authentication (Managed Identity)
public sealed class AzureServiceBusPasswordless
{
    public string Namespace { get; set; }

    public bool IsValid()
    {
        return string.IsNullOrWhiteSpace(Namespace) == false;
    }

    private bool Equals(AzureServiceBusPasswordless other)
    {
        return Namespace == other.Namespace;
    }

    public override bool Equals(object obj)
    {
        return ReferenceEquals(this, obj) || obj is AzureServiceBusPasswordless other && Equals(other);
    }

    public override int GetHashCode()
    {
        return Namespace?.GetHashCode() ?? 0;
    }
}
