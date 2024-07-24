using System.IO;
using Raven.Client.Documents.Operations.Backups;
using Raven.Server.ServerWide;
using Sparrow.Logging;

namespace Raven.Server.Documents.PeriodicBackup.DirectUpload;

public class DirectUploadBackupTask : BackupTask
{
    private readonly DirectBackupUploader _directBackupUploader;

    internal DirectUploadBackupTask(DocumentDatabase database, BackupParameters backupParameters,
        BackupConfiguration configuration, OperationCancelToken token, Logger logger, PeriodicBackupRunner.TestingStuff forTestingPurposes = null) : base(database, backupParameters, configuration, token, logger, forTestingPurposes)
    {
        var uploaderSettings = UploaderSettings.GenerateUploaderSettingForBackup(database, Configuration, _taskName, _isServerWide, _backupToLocalFolder, OnBackupException);
        var destination = BackupConfigurationHelper.GetBackupDestinationForDirectUpload(_backupToLocalFolder, configuration, database.Configuration.Backup);
        _directBackupUploader = new DirectBackupUploader(uploaderSettings, RetentionPolicyParameters, logger, BackupResult, _onProgress, token);
    }

    protected override Stream GetStreamForBackupDestination(string filePath, string folderName, string fileName)
    {
        return _directBackupUploader.StreamForBackupDestination(Database, folderName, fileName);
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
