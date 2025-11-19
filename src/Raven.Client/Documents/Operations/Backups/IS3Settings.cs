namespace Raven.Client.Documents.Operations.Backups;

public interface IS3Settings
{
    public string BucketName { get; set; }
    public string CustomServerUrl { get; set; }
    public bool ForcePathStyle { get; set; }
    public S3StorageClass? StorageClass { get; set; }
    public string AwsAccessKey { get; set; }
    public string AwsSecretKey { get; set; }
    public string AwsSessionToken { get; set; }
    public string AwsRegionName { get; set; }
    public string RemoteFolderName { get; set; }
}
