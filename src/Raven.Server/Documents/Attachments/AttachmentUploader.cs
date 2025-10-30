using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.Backups;
using Raven.Server.Documents.PeriodicBackup;
using Raven.Server.Documents.PeriodicBackup.DirectUpload;
using Raven.Server.ServerWide;
using Sparrow.Server.Logging;

namespace Raven.Server.Documents.Attachments;

public sealed class AttachmentUploader : DirectFileUploader
{
    private readonly string _backupDescription;

    public AttachmentUploader(UploaderSettings settings, RavenLogger logger, OperationCancelToken taskCancelToken) :
        base(settings, retentionPolicyParameters: null, logger, backupResult: GenerateUploadResult(), onProgress: progress => { }, taskCancelToken)
    {
        _backupDescription = $"{nameof(AttachmentUploader)} for identifier '{_settings.TaskName}'";
    }

    public override string GetBackupDescription()
    {
        return _backupDescription;
    }

    public Task<IDictionary<string, string>> GetObjectMetadataAsync(string folderName, string objKeyName)
    {
        switch (_destination)
        {
            case BackupConfiguration.BackupDestination.AmazonS3:
                return GetObjectMetadataFromS3Async(_settings.S3Settings, folderName, objKeyName);

            case BackupConfiguration.BackupDestination.Azure:
                return GetObjectMetadataFromAzureAsync(_settings.AzureSettings, folderName, objKeyName);

            default:
                throw new ArgumentOutOfRangeException($"Missing implementation for direct upload destination '{_destination}'");
        }
    }

    public Task<long?> GetObjectSizeAsync(string folderName, string objKeyName)
    {
        switch (_destination)
        {
            case BackupConfiguration.BackupDestination.AmazonS3:
                return GetObjectSizeS3Async(folderName, objKeyName);

            case BackupConfiguration.BackupDestination.Azure:
                return GetObjectSizeAzureAsync(folderName, objKeyName);

            default:
                throw new ArgumentOutOfRangeException($"Missing implementation for direct upload destination '{_destination}'");
        }
    }

}
