using System;
using System.Linq;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.ETL.Queue;

public sealed class AzureQueueStorageConnectionSettings
{
    public EntraId EntraId { get; set; }

    public string ConnectionString { get; set; }

    public Passwordless Passwordless { get; set; }

    public bool IsValidConnection()
    {
        if (IsOnlyOneConnectionProvided() == false)
        {
            return false;
        }

        if (EntraId != null && EntraId.IsValid() == false)
        {
            return false;
        }

        if (Passwordless != null && Passwordless.IsValid() == false)
        {
            return false;
        }

        return true;
    }
    
    private bool IsOnlyOneConnectionProvided()
    {
        int count = 0;

        if (EntraId != null) 
            count++;
    
        if (!string.IsNullOrWhiteSpace(ConnectionString))
            count++;

        if (Passwordless != null) 
            count++;

        return count == 1;
    }

    public string GetStorageUrl()
    {
        if (ConnectionString != null)
        {
            return GetUrlFromConnectionString(ConnectionString);
        }

        string storageAccountName = GetStorageAccountName();
        return $"https://{storageAccountName}.queue.core.windows.net/";
    }
    
    private string GetUrlFromConnectionString(string connectionString)
    {
        var parts = connectionString.Split(';')
            .Select(p => p.Split('='))
            .ToDictionary(p => p[0], p => p.Length > 1 ? p[1] : "");
        
        parts.TryGetValue("DefaultEndpointsProtocol", out string protocol);
        if (string.IsNullOrWhiteSpace(protocol))
        {
            HandleConnectionStringError("Protocol not found in the connection string");
        }

        if (protocol.Equals("http"))
        {
            parts.TryGetValue("QueueEndpoint", out string queueEndpoint);
            if (string.IsNullOrWhiteSpace(queueEndpoint))
            {
                HandleConnectionStringError("Queue endpoint not found in the connection string");
            }

            return queueEndpoint;
        }

        parts.TryGetValue("AccountName", out string accountName);
        if (string.IsNullOrWhiteSpace(accountName))
        {
            HandleConnectionStringError("Storage account name not found in the connection string");
        }
        
        return $"https://{accountName}.queue.core.windows.net/";
    }

    private string GetStorageAccountName()
    {
        string storageAccountName = "";
        
        if (EntraId != null)
        {
            storageAccountName = EntraId.StorageAccountName;
        }
        else if (Passwordless != null)
        {
            storageAccountName = Passwordless.StorageAccountName;
        }

        return storageAccountName;
    }
    
    private void HandleConnectionStringError(string message)
    {
        throw new ArgumentException(message, nameof(ConnectionString));
    }

    public DynamicJsonValue ToJson()
    {
        var json = new DynamicJsonValue
        {
            [nameof(ConnectionString)] = ConnectionString,
            [nameof(EntraId)] = EntraId == null
                ? null
                : new DynamicJsonValue
                {
                    [nameof(EntraId.StorageAccountName)] = EntraId?.StorageAccountName,
                    [nameof(EntraId.TenantId)] = EntraId?.TenantId,
                    [nameof(EntraId.ClientId)] = EntraId?.ClientId,
                    [nameof(EntraId.ClientSecret)] = EntraId?.ClientSecret
                },
            [nameof(Passwordless)] = Passwordless == null
                ? null
                : new DynamicJsonValue { [nameof(Passwordless.StorageAccountName)] = Passwordless?.StorageAccountName }
        };

        return json;
    }
}

public sealed class EntraId
{
    public string StorageAccountName { get; set; }
    public string TenantId { get; set; }
    public string ClientId { get; set; }
    public string ClientSecret { get; set; }

    public bool IsValid()
    {
        return !string.IsNullOrWhiteSpace(StorageAccountName) &&
               !string.IsNullOrWhiteSpace(TenantId) &&
               !string.IsNullOrWhiteSpace(ClientId) &&
               !string.IsNullOrWhiteSpace(ClientSecret);
    }
}

// this is used for machine authentication
public sealed class Passwordless
{
    public string StorageAccountName { get; set; }

    public bool IsValid()
    {
        return !string.IsNullOrWhiteSpace(StorageAccountName);
    }
}
