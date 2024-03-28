using System;
using System.Linq;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.ETL.Queue;

public sealed class AzureQueueStorageConnectionSettings
{
    public Authentication Authentication;

    public string GetStorageUrl()
    {
        string storageAccountName = GetStorageAccountName();
        return $"https://{storageAccountName}.queue.core.windows.net/";
    }

    public string GetStorageAccountName()
    {
        string storageAccountName = "";

        if (Authentication.ConnectionString != null)
        {
            var accountNamePart = Authentication.ConnectionString.Split(';')
                .FirstOrDefault(part => part.StartsWith("AccountName=", StringComparison.OrdinalIgnoreCase));

            if (accountNamePart == null)
            {
                throw new ArgumentException("Storage account name not found in the connection string.",
                    nameof(Authentication.ConnectionString));
            }

            storageAccountName = accountNamePart.Substring("AccountName=".Length);
        }
        else if (Authentication.EntraId != null)
        {
            storageAccountName = Authentication.EntraId.StorageAccountName;
        }

        return storageAccountName;
    }

    public DynamicJsonValue ToJson()
    {
        var json = new DynamicJsonValue
        {
            [nameof(Authentication)] = Authentication == null
                ? null
                : new DynamicJsonValue
                {
                    [nameof(Authentication.ConnectionString)] = Authentication.ConnectionString,
                    [nameof(Authentication.Passwordless)] = Authentication.Passwordless,
                    [nameof(Authentication.EntraId)] =
                        Authentication.EntraId == null
                            ? null
                            : new DynamicJsonValue
                            {
                                [nameof(Authentication.EntraId.StorageAccountName)] =
                                    Authentication?.EntraId?.StorageAccountName,
                                [nameof(Authentication.EntraId.TenantId)] =
                                    Authentication?.EntraId?.TenantId,
                                [nameof(Authentication.EntraId.ClientId)] =
                                    Authentication?.EntraId?.ClientId,
                                [nameof(Authentication.EntraId.ClientSecret)] =
                                    Authentication?.EntraId?.ClientSecret
                            }
                }
        };

        return json;
    }
}

public sealed class Authentication
{
    public EntraId EntraId { get; set; }
    public string ConnectionString { get; set; }
    public bool Passwordless { get; set; }
}

public sealed class EntraId
{
    public string StorageAccountName { get; set; }
    public string TenantId { get; set; }
    public string ClientId { get; set; }
    public string ClientSecret { get; set; }
}
