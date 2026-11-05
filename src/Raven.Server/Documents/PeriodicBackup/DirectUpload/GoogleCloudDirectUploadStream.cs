using Raven.Server.Documents.PeriodicBackup.GoogleCloud;
using Raven.Server.Documents.PeriodicBackup.Retention;
using Sparrow;

namespace Raven.Server.Documents.PeriodicBackup.DirectUpload;

public class GoogleCloudDirectUploadStream : DirectUploadStream<RavenGoogleCloudClient>
{
    protected override long MinOnePartUploadSizeInBytes { get; }

    public GoogleCloudDirectUploadStream(Parameters parameters) : base(parameters)
    {
        MinOnePartUploadSizeInBytes = Client.DirectUploadPartSize.GetValue(SizeUnit.Bytes);
    }

    protected override void OnCompleteUploadInternal()
    {
        var runner = new GoogleCloudRetentionPolicyRunner(_retentionPolicyParameters, Client);
        runner.Execute();
    }
}
