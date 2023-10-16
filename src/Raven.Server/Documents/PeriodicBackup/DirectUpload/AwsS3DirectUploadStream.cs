using System;
using System.Collections.Generic;
using Raven.Client.Documents.Operations.Backups;
using Raven.Server.Documents.PeriodicBackup.Aws;
using Raven.Server.Documents.PeriodicBackup.Retention;
using Sparrow;
using BackupConfiguration = Raven.Server.Config.Categories.BackupConfiguration;

namespace Raven.Server.Documents.PeriodicBackup.DirectUpload;

public class AwsS3DirectUploadStream : DirectUploadStream
{
    private readonly RavenAwsS3Client _client;
    private readonly RetentionPolicyBaseParameters _retentionPolicyParameters;

    protected override IMultiPartUploader MultiPartUploader { get; }

    protected override long MaxPartSizeInBytes { get; }

    public AwsS3DirectUploadStream(Parameters parameters) : base(parameters.OnProgress)
    {
        _client = new RavenAwsS3Client(parameters.Settings, parameters.Configuration);
        _retentionPolicyParameters = parameters.RetentionPolicyParameters;

        var key = BackupUploader.CombinePathAndKey(parameters.Settings.RemoteFolderName, parameters.FolderName, parameters.FileName);
        MultiPartUploader = _client.GetUploader(key, new Dictionary<string, string>
        {
            {
                "Description", BackupUploader.GetBackupDescription(parameters.BackupType, parameters.IsFullBackup)
            }
        });

        MaxPartSizeInBytes = _client.MinOnePartUploadSizeLimit.GetValue(SizeUnit.Bytes);
    }

    protected override void Dispose(bool disposing)
    {
        using (_client)
        {
            base.Dispose(disposing);

            var runner = new S3RetentionPolicyRunner(_retentionPolicyParameters, _client);
            runner.Execute();
        }
    }

    public class Parameters
    {
        public S3Settings Settings { get; set; }

        public BackupConfiguration Configuration { get; set; }

        public BackupType BackupType { get; set; }

        public bool IsFullBackup { get; set; }

        public string FolderName { get; set; }

        public string FileName { get; set; }

        public RetentionPolicyBaseParameters RetentionPolicyParameters { get; set; }

        public Action<string> OnProgress { get; set; }
    }
}
