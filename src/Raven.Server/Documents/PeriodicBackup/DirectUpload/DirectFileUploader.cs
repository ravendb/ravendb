using System;
using System.Collections.Generic;
using System.IO;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Backups;
using Raven.Server.Documents.PeriodicBackup.Aws;
using Raven.Server.Documents.PeriodicBackup.Azure;
using Raven.Server.Documents.PeriodicBackup.Retention;
using Raven.Server.ServerWide;
using Sparrow.Server.Logging;

namespace Raven.Server.Documents.PeriodicBackup.DirectUpload;

public class DirectFileUploader : FileUploaderBase
{
    private readonly string _backupDescription;
    protected static readonly string Description = "Description";

    public DirectFileUploader(UploaderSettings settings, RetentionPolicyBaseParameters retentionPolicyParameters, RavenLogger logger, BackupResult backupResult, Action<IOperationProgress> onProgress, OperationCancelToken taskCancelToken) :
        base(settings, retentionPolicyParameters, logger, backupResult, onProgress, taskCancelToken)
    {
        var fullBackupText = settings.BackupType == BackupType.Backup ? "Full backup" : "A snapshot";
        _backupDescription = _isFullBackup ? fullBackupText : "Incremental backup";
    }

    internal Stream StreamForBackupDestination(DocumentDatabase database, string folderName, string fileName)
    {
        switch (_destination)
        {
            case BackupConfiguration.BackupDestination.AmazonS3:
                return new AwsS3DirectUploadStream(GetDirectUploadParameters(
                    progress => new RavenAwsS3Client(_settings.S3Settings, database.Configuration.Backup, progress, TaskCancelToken.Token),
                    _settings.S3Settings.RemoteFolderName, folderName, fileName));

            case BackupConfiguration.BackupDestination.Azure:
                return new AzureDirectUploadStream(GetDirectUploadParameters(
                    progress => RavenAzureClient.Create(_settings.AzureSettings, database.Configuration.Backup, progress, TaskCancelToken.Token),
                    _settings.AzureSettings.RemoteFolderName, folderName, fileName));

            default:
                throw new ArgumentOutOfRangeException($"Missing implementation for direct upload destination '{_destination}'");
        }
    }

    private DirectUploadStream<T>.Parameters GetDirectUploadParameters<T>(Func<Progress, T> clientFactory, string remoteFolderName, string folderName, string fileName) where T : IDirectUploader
    {
        return new DirectUploadStream<T>.Parameters
        {
            ClientFactory = clientFactory,
            Key = CombinePathAndKey(remoteFolderName, folderName, fileName),
            Metadata = new Dictionary<string, string>
            {
                { Description, GetBackupDescription() }
            },
            IsFullBackup = _isFullBackup,
            RetentionPolicyParameters = _retentionPolicyParameters,
            CloudUploadStatus = _backupResult.S3Backup,
            OnBackupException = _settings.OnBackupException,
            OnProgress = AddInfo
        };
    }

    public virtual string GetBackupDescription()
    {
        return _backupDescription;
    }
}
