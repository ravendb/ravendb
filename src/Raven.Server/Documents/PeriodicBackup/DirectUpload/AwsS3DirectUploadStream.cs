using System;
using System.Collections.Generic;
using Raven.Client.Documents.Operations.Backups;
using Raven.Server.Documents.PeriodicBackup.Aws;
using Sparrow;
using BackupConfiguration = Raven.Server.Config.Categories.BackupConfiguration;

namespace Raven.Server.Documents.PeriodicBackup.DirectUpload;

public class AwsS3DirectUploadStream : DirectUploadStream
{
    private readonly RavenAwsS3Client _client;

    protected override IMultiPartUploader MultiPartUploader { get; }

    protected override long MaxPartSizeInBytes { get; }

    public AwsS3DirectUploadStream(Parameters parameters)
    {
        var metadata = new Dictionary<string, string>
        {
            { "Description", BackupUploader.GetBackupDescription(parameters.BackupType, parameters.IsFullBackup) }
        };

        _client = new RavenAwsS3Client(parameters.Settings, parameters.Configuration);

        var key = BackupUploader.CombinePathAndKey(parameters.Settings.RemoteFolderName, parameters.FolderName, parameters.FileName);
        MultiPartUploader = _client.GetUploader(key, metadata);
        MaxPartSizeInBytes = _client.MinOnePartUploadSizeLimit.GetValue(SizeUnit.Bytes);

        //TODO:
        //var runner = new S3RetentionPolicyRunner(_retentionPolicyParameters, client);
        //runner.Execute();
    }

    protected override void Dispose(bool disposing)
    {
        base.Dispose(disposing);

        _client.Dispose();
    }

    public class Parameters
    {
        public S3Settings Settings { get; set; }

        public BackupConfiguration Configuration { get; set; }

        public BackupType BackupType { get; set; }

        public bool IsFullBackup { get; set; }

        public string FolderName { get; set; }

        public string FileName { get; set; }

        public Action<string> OnProgress { get; set; }
    }
}
