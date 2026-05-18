namespace Raven.Client.Documents.Operations.Backups;

/// <summary>
/// Defines the settings required for Amazon S3 (or compatible) storage configuration.
/// Shared by periodic backups and remote attachments.
/// </summary>
public interface IS3Settings
{
    /// <summary>
    /// Gets or sets the name of the S3 bucket.
    /// </summary>
    public string BucketName { get; set; }

    /// <summary>
    /// Gets or sets the custom server URL for S3-compatible providers.
    /// </summary>
    public string CustomServerUrl { get; set; }

    /// <summary>
    /// Gets or sets a value indicating whether to force path-style access.
    /// </summary>
    public bool ForcePathStyle { get; set; }

    /// <summary>
    /// Gets or sets the S3 storage class.
    /// </summary>
    public S3StorageClass? StorageClass { get; set; }

    /// <summary>
    /// Gets or sets the AWS Access Key ID.
    /// </summary>
    public string AwsAccessKey { get; set; }

    /// <summary>
    /// Gets or sets the AWS Secret Access Key.
    /// </summary>
    public string AwsSecretKey { get; set; }

    /// <summary>
    /// Gets or sets the AWS Session Token.
    /// </summary>
    public string AwsSessionToken { get; set; }

    /// <summary>
    /// Gets or sets the AWS Region name.
    /// </summary>
    public string AwsRegionName { get; set; }

    /// <summary>
    /// Gets or sets the remote folder path.
    /// </summary>
    public string RemoteFolderName { get; set; }

    /// <summary>
    /// Gets or sets a value indicating whether to disable checksum validation.
    /// </summary>
    public bool DisableChecksumValidation { get; set; }
}
