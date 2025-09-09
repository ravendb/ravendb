using Raven.Server.Documents.PeriodicBackup.Azure;
using Raven.Server.Documents.PeriodicBackup.Retention;
using Sparrow;

namespace Raven.Server.Documents.PeriodicBackup.DirectUpload;

public class AzureDirectUploadStream : DirectUploadStream<IRavenAzureClient>
{
    protected override long MinOnePartUploadSizeInBytes { get; }

    public AzureDirectUploadStream(Parameters parameters) : base(parameters)
    {
        MinOnePartUploadSizeInBytes = Client.MaxSingleBlockSize.GetValue(SizeUnit.Bytes);
    }

    protected override void OnCompleteUploadInternal()
    {
        var runner = new AzureRetentionPolicyRunner(_retentionPolicyParameters, Client);
        runner.Execute();
    }
}
