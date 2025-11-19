using System;
using Raven.Client.Documents.Operations.Backups;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Attachments;

public sealed class RemoteAttachmentsAzureSettings : IRemoteAttachmentsSettings, IAzureSettings, IDynamicJson
{
    public string StorageContainer { get; set; }
    public string RemoteFolderName { get; set; }
    public string AccountName { get; set; }
    public string AccountKey { get; set; }
    public string SasToken { get; set; }

    internal bool HasSettings()
    {
        // Minimal enabling condition – container must be set.
        return string.IsNullOrWhiteSpace(StorageContainer) == false;
    }
    public DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue
        {
            [nameof(StorageContainer)] = StorageContainer,
            [nameof(RemoteFolderName)] = RemoteFolderName,
            [nameof(AccountName)] = AccountName,
            [nameof(AccountKey)] = AccountKey,
            [nameof(SasToken)] = SasToken
        };
    }

    public override int GetHashCode()
    {
        var hc = new HashCode();
        hc.Add(StorageContainer);
        hc.Add(RemoteFolderName);
        hc.Add(AccountName);
        hc.Add(AccountKey);
        hc.Add(SasToken);
        return hc.ToHashCode();
    }

    public override bool Equals(object obj)
    {
        if (ReferenceEquals(this, obj))
            return true;
        if (obj is not RemoteAttachmentsAzureSettings other)
            return false;

        return StorageContainer == other.StorageContainer &&
               RemoteFolderName == other.RemoteFolderName &&
               AccountName == other.AccountName &&
               AccountKey == other.AccountKey &&
               SasToken == other.SasToken;
    }
}
