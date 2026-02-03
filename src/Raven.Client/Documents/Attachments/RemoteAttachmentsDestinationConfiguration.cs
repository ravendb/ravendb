using System;
using Raven.Client.Extensions;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Attachments;

/// <summary>
/// Configuration settings for the destination where remote attachments will be stored.
/// </summary>
/// <remarks>
/// <para>
/// This class defines the cloud storage destination configuration for RavenDB's remote attachments feature.
/// Remote attachments allow offloading large attachment files from the local RavenDB database to external
/// cloud storage providers, reducing database size and improving performance for scenarios with many or large attachments.
/// </para>
/// <para>
/// The configuration supports two mutually exclusive cloud storage providers:
/// <list type="bullet">
/// <item><description>Amazon S3 (via <see cref="S3Settings"/>)</description></item>
/// <item><description>Microsoft Azure Blob Storage (via <see cref="AzureSettings"/>)</description></item>
/// </list>
/// </para>
/// <para>
/// Only one provider can be configured at a time. The configuration will be validated to ensure
/// exactly one uploader is configured when the feature is enabled.
/// </para>
/// </remarks>
public sealed class RemoteAttachmentsDestinationConfiguration : IDynamicJson
{
    /// <summary>
    /// Gets or sets a value indicating whether remote attachments functionality is disabled for this destination.
    /// </summary>
    /// <remarks>
    /// <para>
    /// When set to <c>true</c>, the remote attachments feature is disabled and attachments will not be
    /// uploaded to the configured cloud storage destination. Attachments will be stored locally in the database.
    /// </para>
    /// <para>
    /// When set to <c>false</c>, attachments will be uploaded to the configured cloud storage provider
    /// (either S3 or Azure) according to the settings specified in <see cref="S3Settings"/> or <see cref="AzureSettings"/>.
    /// </para>
    /// </remarks>
    /// <value><c>true</c> if remote attachments are disabled; otherwise, <c>false</c>. Default is <c>false</c>.</value>
    public bool Disabled { get; set; }

    /// <summary>
    /// Gets or sets the Amazon S3 storage configuration for remote attachments.
    /// </summary>
    /// <remarks>
    /// <para>
    /// This property configures Amazon S3 as the destination for remote attachments. When configured,
    /// attachments will be uploaded to the specified S3 bucket using the provided credentials.
    /// </para>
    /// <para>
    /// This setting is mutually exclusive with <see cref="AzureSettings"/>. Only one cloud storage
    /// provider can be configured at a time. The configuration will be validated to ensure this constraint.
    /// </para>
    /// </remarks>
    /// <value>
    /// The S3 settings configuration or <c>null</c> if Azure is being used instead.
    /// </value>
    /// <seealso cref="RemoteAttachmentsS3Settings"/>
    /// <seealso cref="AzureSettings"/>
    public RemoteAttachmentsS3Settings S3Settings { get; set; }

    /// <summary>
    /// Gets or sets the Microsoft Azure Blob Storage configuration for remote attachments.
    /// </summary>
    /// <remarks>
    /// <para>
    /// This property configures Azure Blob Storage as the destination for remote attachments. When configured,
    /// attachments will be uploaded to the specified Azure storage container using the provided credentials.
    /// </para>
    /// <para>
    /// This setting is mutually exclusive with <see cref="S3Settings"/>. Only one cloud storage
    /// provider can be configured at a time. The configuration will be validated to ensure this constraint.
    /// </para>
    /// </remarks>
    /// <value>
    /// The Azure Blob Storage settings configuration or <c>null</c> if S3 is being used instead.
    /// </value>
    /// <seealso cref="RemoteAttachmentsAzureSettings"/>
    /// <seealso cref="S3Settings"/>
    public RemoteAttachmentsAzureSettings AzureSettings { get; set; }

    public override int GetHashCode()
    {
        var hashCode = new HashCode();
        hashCode.Add(Disabled);

        hashCode.Add(S3Settings != null ? S3Settings.GetHashCode() : 0);
        hashCode.Add(AzureSettings != null ? AzureSettings.GetHashCode() : 0);

        return hashCode.ToHashCode();
    }

    public override bool Equals(object obj)
    {
        if (ReferenceEquals(null, obj))
            return false;
        if (ReferenceEquals(this, obj))
            return true;
        if (obj.GetType() != GetType())
            return false;
        return Equals((RemoteAttachmentsDestinationConfiguration)obj);
    }

    private bool Equals(RemoteAttachmentsDestinationConfiguration other)
    {
        if (Disabled != other.Disabled)
            return false;

        if (S3Settings != null)
        {
            if (other.S3Settings == null)
                return false;
            if (S3Settings.Equals(other.S3Settings) == false)
                return false;
        }
        if (S3Settings == null && other.S3Settings != null)
        {
            return false;
        }

        if (AzureSettings != null)
        {
            if (other.AzureSettings == null)
                return false;
            if (AzureSettings.Equals(other.AzureSettings) == false)
                return false;
        }
        if (AzureSettings == null && other.AzureSettings != null)
        {
            return false;
        }

        return true;
    }

    public DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue
        {
            [nameof(Disabled)] = Disabled,
            [nameof(S3Settings)] = S3Settings?.ToJson(),
            [nameof(AzureSettings)] = AzureSettings?.ToJson(),
        };
    }

    internal DynamicJsonValue ToStudioJson()
    {
        return new DynamicJsonValue
        {
            [nameof(Disabled)] = Disabled
        };
    }

    internal bool HasUploader()
    {
        if (Disabled)
            return false;

        return S3Settings.IsConfigured() || AzureSettings.IsConfigured();
    }

    internal void AssertConfiguration(string key, string databaseName = null)
    {
        var databaseNameStr = string.IsNullOrEmpty(databaseName) ? string.Empty : $" for database '{databaseName}'";

        if (S3Settings.IsConfigured() == false && AzureSettings.IsConfigured() == false)
            throw new InvalidOperationException($"Exactly one uploader for {nameof(RemoteAttachmentsConfiguration)}{databaseNameStr} must be configured.");
        if (S3Settings.IsConfigured() && AzureSettings.IsConfigured())
            throw new InvalidOperationException($"Only one uploader for {nameof(RemoteAttachmentsConfiguration)}{databaseNameStr} can be configured.");
    }
}
