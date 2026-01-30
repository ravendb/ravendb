using System;
using Raven.Client.Documents.Operations.Backups;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Attachments;

/// <summary>
/// Configuration settings for storing attachments in Azure Blob Storage as part of the remote attachments feature.
/// </summary>
/// <remarks>
/// <para>
/// This class configures the remote storage destination for attachments using Microsoft Azure Blob Storage.
/// Remote attachments allow offloading large attachment files from the local RavenDB database to cloud storage,
/// helping to reduce database size and improve performance for scenarios with many or large attachments.
/// </para>
/// <para>
/// Azure authentication can be configured using either:
/// <list type="bullet">
/// <item><description>Account Key: Using <see cref="AccountName"/> and <see cref="AccountKey"/></description></item>
/// <item><description>Shared Access Signature (SAS): Using <see cref="AccountName"/> and <see cref="SasToken"/></description></item>
/// </list>
/// </para>
/// <para>
/// This class is typically used in conjunction with <see cref="RemoteAttachmentsDestinationConfiguration"/> to define
/// where and how attachments should be uploaded to Azure Blob Storage.
/// </para>
/// </remarks>
public sealed class RemoteAttachmentsAzureSettings : IRemoteAttachmentsSettings, IAzureSettings, IDynamicJson
{
    /// <summary>
    /// Gets or sets the name of the Azure Blob Storage container where attachments will be stored.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The storage container name must follow Azure Blob Storage naming rules:
    /// <list type="bullet">
    /// <item><description>Must be between 3 and 63 characters long</description></item>
    /// <item><description>Must contain only lowercase letters, numbers, and dashes</description></item>
    /// <item><description>Must start and end with a letter or number</description></item>
    /// <item><description>Cannot contain consecutive dashes</description></item>
    /// </list>
    /// </para>
    /// <para>
    /// This property is required for the configuration to be valid and determines the primary location
    /// where attachments will be stored within the Azure storage account.
    /// </para>
    /// </remarks>
    /// <value>The name of the Azure storage container.</value>
    public string StorageContainer { get; set; }

    /// <summary>
    /// Gets or sets the optional subfolder path within the storage container for organizing attachments.
    /// </summary>
    /// <remarks>
    /// <para>
    /// This property allows you to organize attachments into a specific folder structure within the Azure container.
    /// For example, you might use different folder names for different databases, environments, or tenants.
    /// </para>
    /// <para>
    /// The folder path can include forward slashes to create a multi-level directory structure.
    /// If not specified, attachments will be stored in the root of the container.
    /// </para>
    /// </remarks>
    /// <value>The remote folder name or <c>null</c> to use the container root.</value>
    /// <example>"production/database1" or "attachments/2024"</example>
    public string RemoteFolderName { get; set; }

    /// <summary>
    /// Gets or sets the Azure storage account name.
    /// </summary>
    /// <remarks>
    /// <para>
    /// This is the name of your Azure storage account and is required for authentication.
    /// The account name is used in combination with either <see cref="AccountKey"/> or <see cref="SasToken"/>
    /// to authenticate with Azure Blob Storage.
    /// </para>
    /// <para>
    /// The storage account must have the appropriate permissions to create and write blobs to the specified container.
    /// </para>
    /// </remarks>
    /// <value>The Azure storage account name.</value>
    public string AccountName { get; set; }

    /// <summary>
    /// Gets or sets the Azure storage account key for authentication.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The account key provides full access to the storage account. Use this property when authenticating
    /// with the primary or secondary access key from your Azure storage account.
    /// </para>
    /// <para>
    /// You should use either <see cref="AccountKey"/> or <see cref="SasToken"/> for authentication, but not both.
    /// Account keys provide broader access than SAS tokens, so consider using a SAS token with limited permissions
    /// if that better fits your security requirements.
    /// </para>
    /// </remarks>
    /// <value>The Azure storage account key or <c>null</c> if using <see cref="SasToken"/>.</value>
    public string AccountKey { get; set; }

    /// <summary>
    /// Gets or sets the Shared Access Signature (SAS) token for limited-scope authentication.
    /// </summary>
    /// <remarks>
    /// <para>
    /// A SAS token provides delegated access to resources in your storage account with granular control
    /// over permissions, expiration time, and allowed IP addresses. This is often preferred over using
    /// an account key for security reasons.
    /// </para>
    /// <para>
    /// The SAS token should have at minimum the following permissions for the specified container:
    /// <list type="bullet">
    /// <item><description>Write (w): To upload attachments</description></item>
    /// <item><description>Read (r): To download attachments when needed</description></item>
    /// <item><description>List (l): To enumerate attachments if required</description></item>
    /// </list>
    /// </para>
    /// <para>
    /// You should use either <see cref="SasToken"/> or <see cref="AccountKey"/> for authentication, but not both.
    /// </para>
    /// </remarks>
    /// <value>The SAS token or <c>null</c> if using <see cref="AccountKey"/>.</value>
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
