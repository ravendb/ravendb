using System;
using Raven.Client.Documents.Operations.Backups;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Attachments;

public sealed class RemoteAttachmentsS3Settings : IS3Settings, IRemoteAttachmentsSettings, IDynamicJson
{
    public string AwsAccessKey { get; set; }
    public string AwsSecretKey { get; set; }
    public string AwsSessionToken { get; set; }
    public string AwsRegionName { get; set; }
    public string RemoteFolderName { get; set; }

    public string BucketName { get; set; }
    public string CustomServerUrl { get; set; }
    public bool ForcePathStyle { get; set; }
    public S3StorageClass? StorageClass { get; set; }

    internal bool HasSettings()
    {
        // Minimal enabling condition – bucket must be set.
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
            [nameof(ForcePathStyle)] = ForcePathStyle
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
               StorageClass == other.StorageClass &&
               AwsAccessKey == other.AwsAccessKey &&
               AwsSecretKey == other.AwsSecretKey &&
               AwsSessionToken == other.AwsSessionToken;
    }
}

public interface IRemoteAttachmentsSettings
{
    public string RemoteFolderName { get; set; }
}
