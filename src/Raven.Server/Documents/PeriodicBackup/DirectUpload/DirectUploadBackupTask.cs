using System.IO;
using Raven.Client.Documents.Operations.Backups;
using Raven.Server.ServerWide;
using Sparrow.Logging;

namespace Raven.Server.Documents.PeriodicBackup.DirectUpload;

public partial class DirectUploadBackupTask : BackupTask
{
    internal DirectUploadBackupTask(DocumentDatabase database, BackupParameters backupParameters,
        BackupConfiguration configuration, OperationCancelToken token, Logger logger, PeriodicBackupRunner.TestingStuff forTestingPurposes = null) : base(database, backupParameters, configuration, token, logger, forTestingPurposes)
    {
    }

    protected override BackupDestinationStream GetUploaderForBackupDestination(string filePath, string folderName, string fileName)
    {
        var uploaderSettings = UploaderSettings.GenerateUploaderSettingForBackup(Database, Configuration, _taskName, _isServerWide, _backupToLocalFolder, OnBackupException);
        var backupUploader = new DirectBackupUploader(uploaderSettings, RetentionPolicyParameters, _logger, BackupResult, _onProgress, TaskCancelToken);
        return new BackupDestinationStream()
        {
            Stream = backupUploader.StreamForBackupDestination(Database, folderName, fileName), BackupUploader = backupUploader
        };
    }

    protected override void UploadToServer(string backupFilePath, string folderName, string fileName)
    {
        // uploading was already done while generating the backup file.
    }

    protected override void ValidateFreeSpaceForSnapshot(string filePath)
    {
        // we're uploading directly without using a local file.
    }

    protected override void RenameFile(string backupFilePath, string tempBackupFilePath)
    {
        // we're uploading directly without using a local file.
    }

    protected override void DeleteFile(string path)
    {
        // we're uploading directly without using a local file.
    }

    protected override void FlushToDisk(Stream outputStream)
    {
        // no need to flush since we're uploading directly without using a local file.
    }
}
