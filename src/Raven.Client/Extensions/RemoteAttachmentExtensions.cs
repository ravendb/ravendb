using System;
using Raven.Client.Documents.Attachments;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Client.Documents.Operations.Backups;
using Sparrow.Json;

namespace Raven.Client.Extensions;

/// <summary>
/// Provides extension methods for working with attachment remote parameters.
/// </summary>
internal static class RemoteAttachmentExtensions
{
    /// <summary>
    /// Determines whether the attachment parameters indicate a local (non-remote) attachment.
    /// </summary>
    /// <param name="parameters">The remote parameters to check. Can be null.</param>
    /// <returns>
    /// <c>true</c> if the parameters are null or have flags set to None (<see cref="RemoteAttachmentFlags.None"/>), indicating a local storage attachment;
    /// otherwise, <c>false</c>.
    /// </returns>
    public static bool IsLocalStorageAttachment(this RemoteAttachmentParameters parameters)
    {
        return parameters == null || parameters.Flags == RemoteAttachmentFlags.None;
    }

    /// <summary>
    /// Determines whether the attachment parameters indicate a remote storage attachment.
    /// </summary>
    /// <param name="parameters">The remote parameters to check. Can be null.</param>
    /// <returns>
    /// <c>true</c> if the parameters are not null and have flags set to Remote (<see cref="RemoteAttachmentFlags.Remote"/>), indicating a remote cloud storage attachment.
    /// otherwise, <c>false</c>.
    /// </returns>
    public static bool IsRemoteStorageAttachment(this RemoteAttachmentParameters parameters)
    {
        if (parameters == null)
        {
            return false;
        }

        return parameters.Flags == RemoteAttachmentFlags.Remote;
    }

    private static RemoteAttachmentParameters GetRemoteAttachmentParameters(string identifier, DateTime? remoteAt, RemoteAttachmentFlags flags)
    {
        RemoteAttachmentParameters remoteParameters = null;
        if (remoteAt.HasValue)
        {
            remoteParameters = new RemoteAttachmentParameters(identifier, remoteAt.Value) { Flags = flags };
        }

        return remoteParameters;
    }

    internal static RemoteAttachmentParameters GetRemoteAttachmentParameters(LazyStringValue identifier, DateTime? remoteAt, RemoteAttachmentFlags flags)
    {
        return GetRemoteAttachmentParameters(identifier.ToString(), remoteAt, flags);
    }

    public static bool IsConfigured(this RemoteAttachmentsAzureSettings settings)
    {
        return settings != null && settings.HasSettings();
    }

    public static bool IsConfigured(this RemoteAttachmentsS3Settings settings)
    {
        return settings != null && settings.HasSettings();
    }

    /// <summary>
    /// Converts periodic backup <see cref="S3Settings"/> to remote attachments <see cref="RemoteAttachmentsS3Settings"/>.
    /// Ignores backup-specific fields (Disabled, scripts).
    /// </summary>
    public static RemoteAttachmentsS3Settings ToRemoteAttachmentsS3Settings(this S3Settings settings)
    {
        if (settings == null)
            return null;

        return new RemoteAttachmentsS3Settings
        {
            AwsAccessKey = settings.AwsAccessKey,
            AwsSecretKey = settings.AwsSecretKey,
            AwsSessionToken = settings.AwsSessionToken,
            AwsRegionName = settings.AwsRegionName,
            RemoteFolderName = settings.RemoteFolderName,
            BucketName = settings.BucketName,
            CustomServerUrl = settings.CustomServerUrl,
            ForcePathStyle = settings.ForcePathStyle,
            DisableChecksumValidation = settings.DisableChecksumValidation,
            StorageClass = settings.StorageClass
        };
    }

    /// <summary>
    /// Converts periodic backup <see cref="AzureSettings"/> to remote attachments <see cref="RemoteAttachmentsAzureSettings"/>.
    /// Ignores backup-specific fields (Disabled, scripts).
    /// </summary>
    public static RemoteAttachmentsAzureSettings ToRemoteAttachmentsAzureSettings(this AzureSettings settings)
    {
        if (settings == null)
            return null;

        return new RemoteAttachmentsAzureSettings
        {
            StorageContainer = settings.StorageContainer,
            RemoteFolderName = settings.RemoteFolderName,
            AccountName = settings.AccountName,
            AccountKey = settings.AccountKey,
            SasToken = settings.SasToken
        };
    }

    /// <summary>
    /// Converts remote attachments <see cref="RemoteAttachmentsS3Settings"/> back to periodic backup <see cref="S3Settings"/>.
    /// Returns null if remote settings are null or not minimally configured.
    /// </summary>
    public static S3Settings ToS3Settings(this RemoteAttachmentsS3Settings settings)
    {
        if (settings == null || settings.HasSettings() == false)
            return null;

        return new S3Settings
        {
            AwsAccessKey = settings.AwsAccessKey,
            AwsSecretKey = settings.AwsSecretKey,
            AwsSessionToken = settings.AwsSessionToken,
            AwsRegionName = settings.AwsRegionName,
            RemoteFolderName = settings.RemoteFolderName,
            BucketName = settings.BucketName,
            CustomServerUrl = settings.CustomServerUrl,
            ForcePathStyle = settings.ForcePathStyle,
            DisableChecksumValidation = settings.DisableChecksumValidation,
            StorageClass = settings.StorageClass,
            Disabled = false // ensure enabled for direct upload use-case
        };
    }

    /// <summary>
    /// Converts remote attachments <see cref="RemoteAttachmentsAzureSettings"/> back to periodic backup <see cref="AzureSettings"/>.
    /// Returns null if remote settings are null or not minimally configured.
    /// </summary>
    public static AzureSettings ToAzureSettings(this RemoteAttachmentsAzureSettings settings)
    {
        if (settings == null || settings.HasSettings() == false)
            return null;

        return new AzureSettings
        {
            StorageContainer = settings.StorageContainer,
            RemoteFolderName = settings.RemoteFolderName,
            AccountName = settings.AccountName,
            AccountKey = settings.AccountKey,
            SasToken = settings.SasToken,
            Disabled = false
        };
    }
}
