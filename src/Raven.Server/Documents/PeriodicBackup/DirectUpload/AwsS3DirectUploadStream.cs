using Raven.Server.Documents.PeriodicBackup.Aws;
using Raven.Server.Documents.PeriodicBackup.Retention;
using Sparrow;

namespace Raven.Server.Documents.PeriodicBackup.DirectUpload;

public class AwsS3DirectUploadStream : DirectUploadStream<RavenAwsS3Client>
{
    private readonly RetentionPolicyBaseParameters _retentionPolicyParameters;

    protected override long MaxPartSizeInBytes { get; }

    public AwsS3DirectUploadStream(Parameters parameters) : base(parameters)
    {
        _retentionPolicyParameters = parameters.RetentionPolicyParameters;

        MaxPartSizeInBytes = Client.MinOnePartUploadSizeLimit.GetValue(SizeUnit.Bytes);
    }

    protected override void Dispose(bool disposing)
    {
        using (Client)
        {
            base.Dispose(disposing);

            var runner = new S3RetentionPolicyRunner(_retentionPolicyParameters, Client);
            runner.Execute();
        }
    }
}
