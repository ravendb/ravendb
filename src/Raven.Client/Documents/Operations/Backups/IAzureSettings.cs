namespace Raven.Client.Documents.Operations.Backups
{
    /// <summary>
    /// Defines the settings required for Azure Blob Storage configuration.
    /// Shared by periodic backups and remote attachments.
    /// </summary>
    public interface IAzureSettings
    {
        /// <summary>
        /// Gets or sets the name of the Azure storage container.
        /// </summary>
        string StorageContainer { get; set; }

        /// <summary>
        /// Gets or sets the remote folder path.
        /// </summary>
        string RemoteFolderName { get; set; }

        /// <summary>
        /// Gets or sets the Azure storage account name.
        /// </summary>
        string AccountName { get; set; }

        /// <summary>
        /// Gets or sets the Azure account key.
        /// </summary>
        string AccountKey { get; set; }

        /// <summary>
        /// Gets or sets the Shared Access Signature (SAS) token.
        /// </summary>
        string SasToken { get; set; }
    }
}
