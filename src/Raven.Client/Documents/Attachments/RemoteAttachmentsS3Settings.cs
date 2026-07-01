using System;
using Raven.Client.Documents.Operations.Backups;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Attachments;

/// <summary>
/// Configuration settings for storing remote attachments in Amazon S3 or S3-compatible storage.
/// </summary>
/// <remarks>
/// <para>
/// This class provides the configuration required to upload and store RavenDB attachments in Amazon S3
/// or S3-compatible object storage services. It implements <see cref="IS3Settings"/> for S3-specific settings,
/// <see cref="IRemoteAttachmentsSettings"/> for remote storage configuration, and <see cref="IDynamicJson"/>
/// for JSON serialization support.
/// </para>
/// <para>
/// The configuration supports authentication via AWS access keys, secret keys, and optional session tokens
/// for temporary credentials. It also allows customization of storage class, server URL, and path style options
/// for compatibility with various S3-compatible storage providers.
/// </para>
/// </remarks>
public sealed class RemoteAttachmentsS3Settings : IS3Settings, IRemoteAttachmentsSettings, IDynamicJson
{
    /// <summary>
    /// Gets or sets the AWS access key ID used for authentication with Amazon S3.
    /// </summary>
    /// <remarks>
    /// <para>
    /// This is the first part of the AWS credentials pair (Access Key ID and Secret Access Key).
    /// The access key ID is used to identify the AWS account or IAM user making requests to S3.
    /// </para>
    /// <para>
    /// For security best practices, consider using IAM roles or temporary credentials via
    /// <see cref="AwsSessionToken"/> instead of long-term access keys when possible.
    /// </para>
    /// </remarks>
    /// <value>The AWS access key ID, or <c>null</c> if using alternative authentication methods.</value>
    /// <seealso cref="AwsSecretKey"/>
    /// <seealso cref="AwsSessionToken"/>
    public string AwsAccessKey { get; set; }

    /// <summary>
    /// Gets or sets the AWS secret access key used for authentication with Amazon S3.
    /// </summary>
    /// <remarks>
    /// <para>
    /// This is the second part of the AWS credentials pair (Access Key ID and Secret Access Key).
    /// The secret key is used to sign requests to AWS services and should be kept confidential.
    /// </para>
    /// <para>
    /// <strong>Security Warning:</strong> Never commit secret keys to source control or expose them
    /// in logs or error messages. Store them securely using environment variables, configuration
    /// management systems, or secret management services.
    /// </para>
    /// </remarks>
    /// <value>The AWS secret access key, or <c>null</c> if using alternative authentication methods.</value>
    /// <seealso cref="AwsAccessKey"/>
    public string AwsSecretKey { get; set; }

    /// <summary>
    /// Gets or sets the AWS session token for temporary security credentials.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Session tokens are used when working with temporary security credentials obtained from
    /// AWS Security Token Service (STS). These are typically used with IAM roles, federated users,
    /// or when assuming roles across AWS accounts.
    /// </para>
    /// <para>
    /// Temporary credentials automatically expire after a specified duration, providing enhanced
    /// security compared to long-term access keys. This is the recommended authentication method
    /// for production environments.
    /// </para>
    /// </remarks>
    /// <value>
    /// The AWS session token for temporary credentials, or <c>null</c> if using long-term credentials.
    /// </value>
    /// <seealso cref="AwsAccessKey"/>
    /// <seealso cref="AwsSecretKey"/>
    public string AwsSessionToken { get; set; }

    /// <summary>
    /// Gets or sets the AWS region name where the S3 bucket is located.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The region name specifies the geographic location of the S3 bucket. Common region names include:
    /// <list type="bullet">
    /// <item><description>us-east-1 (US East, N. Virginia)</description></item>
    /// <item><description>us-west-2 (US West, Oregon)</description></item>
    /// <item><description>eu-west-1 (Europe, Ireland)</description></item>
    /// <item><description>ap-southeast-1 (Asia Pacific, Singapore)</description></item>
    /// </list>
    /// </para>
    /// <para>
    /// For S3-compatible storage providers that don't use AWS regions, this may be used to
    /// specify alternative region identifiers or left empty depending on the provider's requirements.
    /// </para>
    /// </remarks>
    /// <value>The AWS region name (e.g., "us-east-1"), or <c>null</c> for default or S3-compatible services.</value>
    public string AwsRegionName { get; set; }

    /// <summary>
    /// Gets or sets the remote folder name (prefix) where attachments will be stored within the S3 bucket.
    /// </summary>
    /// <remarks>
    /// <para>
    /// This property specifies a folder path or key prefix within the S3 bucket where attachment objects
    /// will be stored. It helps organize attachments and can be used to separate different databases,
    /// environments, or tenants within the same bucket.
    /// </para>
    /// </remarks>
    /// <value>The folder name or key prefix, or <c>null</c> to store attachments at the bucket root.</value>
    public string RemoteFolderName { get; set; }

    /// <summary>
    /// Gets or sets a value indicating whether to disable checksum validation.
    /// </summary>
    /// <remarks>
    /// <para>
    /// This property disables checksum validation for S3 uploads.
    /// Checksum Validation ensures data integrity and should not be disabled if not necessary.
    /// </para>
    /// <para>
    /// Set this to <c>true</c> if your S3-compatible storage does not support modern object integrity checks.
    /// </para>
    /// </remarks>
    public bool DisableChecksumValidation { get; set; }

    /// <summary>
    /// Gets or sets the name of the S3 bucket where attachments will be stored.
    /// </summary>
    /// <remarks>
    /// <para>
    /// This is a required property. The bucket must exist and the configured credentials must have
    /// the necessary permissions to upload, download, and list objects in the bucket.
    /// </para>
    /// <para>
    /// Bucket names must follow AWS S3 naming rules:
    /// <list type="bullet">
    /// <item><description>Must be globally unique across all AWS accounts</description></item>
    /// <item><description>Must be between 3 and 63 characters long</description></item>
    /// <item><description>Must consist only of lowercase letters, numbers, dots, and hyphens</description></item>
    /// <item><description>Must begin and end with a letter or number</description></item>
    /// </list>
    /// </para>
    /// </remarks>
    /// <value>The S3 bucket name, or <c>null</c> if not configured.</value>
    public string BucketName { get; set; }

    /// <summary>
    /// Gets or sets a custom server URL for S3-compatible storage providers.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Use this property when connecting to S3-compatible storage providers other than AWS S3,
    /// such as MinIO, Wasabi, DigitalOcean Spaces, or on-premises S3-compatible solutions.
    /// </para>
    /// <para>
    /// The URL should include the protocol (http:// or https://) and may include a port number.
    /// For example: "https://s3.example.com:9000" or "https://nyc3.digitaloceanspaces.com".
    /// </para>
    /// <para>
    /// When using AWS S3, leave this property <c>null</c> to use the default AWS S3 endpoints
    /// determined by the <see cref="AwsRegionName"/>.
    /// </para>
    /// </remarks>
    /// <value>
    /// The custom server URL for S3-compatible storage, or <c>null</c> to use AWS S3 default endpoints.
    /// </value>
    /// <example>
    /// <code>
    /// // Configure for MinIO
    /// s3Settings.CustomServerUrl = "https://minio.example.com:9000";
    /// s3Settings.ForcePathStyle = true;
    /// </code>
    /// </example>
    public string CustomServerUrl { get; set; }

    /// <summary>
    /// Gets or sets a value indicating whether to use path-style addressing for S3 requests.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Path-style URLs include the bucket name in the URL path: http://s3.amazonaws.com/bucket/key
    /// Virtual-hosted-style URLs (default) include the bucket name as a subdomain: http://bucket.s3.amazonaws.com/key
    /// </para>
    /// <para>
    /// Set this to <c>true</c> when:
    /// <list type="bullet">
    /// <item><description>Working with S3-compatible storage that requires path-style access (e.g., MinIO)</description></item>
    /// <item><description>Using buckets with dots in their names that may cause SSL certificate validation issues</description></item>
    /// <item><description>Required by specific S3-compatible storage provider configurations</description></item>
    /// </list>
    /// </para>
    /// <para>
    /// Note: AWS S3 is deprecating path-style access, but it remains necessary for many S3-compatible services.
    /// </para>
    /// </remarks>
    /// <value>
    /// <c>true</c> to use path-style addressing; <c>false</c> to use virtual-hosted-style (default).
    /// </value>
    /// <seealso cref="CustomServerUrl"/>
    public bool ForcePathStyle { get; set; }

    /// <summary>
    /// Gets or sets the S3 storage class to use for stored attachments.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The storage class determines the availability, durability, and cost of stored objects.
    /// Different storage classes are optimized for different access patterns:
    /// </para>
    /// <list type="bullet">
    /// <item><description><see cref="S3StorageClass.Glacier"/> - Low-cost archival storage with retrieval times of minutes to hours</description></item>
    /// <item><description><see cref="S3StorageClass.GlacierInstantRetrieval"/> - Archival storage with millisecond retrieval</description></item>
    /// <item><description><see cref="S3StorageClass.DeepArchive"/> - Lowest-cost storage for long-term archival with retrieval times of up to 12 hours</description></item>
    /// </list>
    /// <para>
    /// Set this to <c>null</c> to use the default storage class (STANDARD). Choose archival storage classes
    /// for cost optimization when attachments are accessed infrequently.
    /// </para>
    /// </remarks>
    /// <value>
    /// The S3 storage class for attachments, or <c>null</c> to use the default STANDARD storage class.
    /// </value>
    /// <seealso cref="S3StorageClass"/>
    public S3StorageClass? StorageClass { get; set; }

    internal bool HasSettings()
    {
        // Minimal enabling condition � bucket must be set.
        return string.IsNullOrWhiteSpace(BucketName) == false;
    }

    public DynamicJsonValue ToJson()
    {
        var djv = new DynamicJsonValue
        {
            [nameof(AwsAccessKey)] = AwsAccessKey,
            [nameof(AwsSecretKey)] = AwsSecretKey,
            [nameof(AwsSessionToken)] = AwsSessionToken,
            [nameof(AwsRegionName)] = AwsRegionName,
            [nameof(RemoteFolderName)] = RemoteFolderName,
            [nameof(BucketName)] = BucketName,
            [nameof(CustomServerUrl)] = CustomServerUrl,
            [nameof(ForcePathStyle)] = ForcePathStyle,
            [nameof(DisableChecksumValidation)] = DisableChecksumValidation
        };

        if (StorageClass.HasValue)
            djv[nameof(StorageClass)] = StorageClass.Value;

        return djv;
    }

    public override int GetHashCode()
    {
        var hc = new HashCode();
        hc.Add(AwsRegionName);
        hc.Add(BucketName);
        hc.Add(RemoteFolderName);
        hc.Add(CustomServerUrl);
        hc.Add(ForcePathStyle);
        hc.Add(DisableChecksumValidation);
        hc.Add(StorageClass);
        hc.Add(AwsAccessKey);
        hc.Add(AwsSecretKey);
        hc.Add(AwsSessionToken);
        return hc.ToHashCode();
    }

    public override bool Equals(object obj)
    {
        if (ReferenceEquals(this, obj))
            return true;
        if (obj is not RemoteAttachmentsS3Settings other)
            return false;

        return AwsRegionName == other.AwsRegionName &&
               BucketName == other.BucketName &&
               RemoteFolderName == other.RemoteFolderName &&
               CustomServerUrl == other.CustomServerUrl &&
               ForcePathStyle == other.ForcePathStyle &&
               DisableChecksumValidation == other.DisableChecksumValidation &&
               StorageClass == other.StorageClass &&
               AwsAccessKey == other.AwsAccessKey &&
               AwsSecretKey == other.AwsSecretKey &&
               AwsSessionToken == other.AwsSessionToken;
    }
}

/// <summary>
/// Defines the contract for remote attachment storage settings.
/// </summary>
/// <remarks>
/// This interface provides a common abstraction for remote attachment storage configuration
/// across different cloud storage providers. It defines the basic property required by all
/// remote attachment storage implementations to organize attachments within the remote storage.
/// </remarks>
public interface IRemoteAttachmentsSettings
{
    /// <summary>
    /// Gets or sets the remote folder name (prefix) where attachments will be stored.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The folder name serves as a logical organization structure within the remote storage,
    /// allowing attachments from different databases, environments, or tenants to be stored
    /// in separate locations within the same storage account or bucket.
    /// </para>
    /// <para>
    /// The actual implementation of how this folder name is used depends on the specific
    /// storage provider (e.g., as an S3 key prefix or Azure Blob Storage container/folder path).
    /// </para>
    /// </remarks>
    /// <value>
    /// The folder name or path prefix for storing attachments, or <c>null</c> to use the root location.
    /// </value>
    public string RemoteFolderName { get; set; }
}
