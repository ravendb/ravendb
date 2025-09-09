using Raven.Server.Documents.PeriodicBackup.Aws;
using Raven.Server.Documents.PeriodicBackup.Retention;
using Sparrow;

namespace Raven.Server.Documents.PeriodicBackup.DirectUpload;

public class AwsS3DirectUploadStream : DirectUploadStream<RavenAwsS3Client>
{
    protected override long MinOnePartUploadSizeInBytes { get; }

    public AwsS3DirectUploadStream(Parameters parameters) : base(parameters)
    {
        MinOnePartUploadSizeInBytes = Client.MinOnePartUploadSizeLimit.GetValue(SizeUnit.Bytes);
    }

    protected override void OnCompleteUploadInternal()
    {
        var runner = new S3RetentionPolicyRunner(_retentionPolicyParameters, Client);
        runner.Execute();
    }
}
