namespace Raven.Client.Documents.Operations.Backups
{
    // Common Azure settings interface (parallel to IS3Settings).
    // Shared by periodic backup AzureSettings and remote attachments RemoteAttachmentsAzureSettings.
    public interface IAzureSettings
    {
        string StorageContainer { get; set; }
        string RemoteFolderName { get; set; }
        string AccountName { get; set; }
        string AccountKey { get; set; }
        string SasToken { get; set; }
    }
}
