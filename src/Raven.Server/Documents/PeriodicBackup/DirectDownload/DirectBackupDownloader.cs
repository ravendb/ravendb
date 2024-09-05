using System;
using System.IO;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Backups;
using Raven.Server.Documents.PeriodicBackup.Restore;
using Raven.Server.Documents.PeriodicBackup.Retention;
using Raven.Server.ServerWide;
using Sparrow.Logging;

namespace Raven.Server.Documents.PeriodicBackup.DirectDownload;

public sealed class DirectBackupDownloader : BackupUploaderBase, IDisposable
{
    private readonly BackupConfiguration.BackupDestination _destination;
    private IRestoreSource _restoreSource = null;
    public DirectBackupDownloader(UploaderSettings settings, RetentionPolicyBaseParameters retentionPolicyParameters, Logger logger, BackupResult backupResult, Action<IOperationProgress> onProgress, OperationCancelToken taskCancelToken) :
        base(settings, retentionPolicyParameters, logger, backupResult, onProgress, taskCancelToken)
    {
        _destination = settings.Destination;
    }

    internal async Task<Stream> StreamForDownloadDestination(DocumentDatabase database, string folderName, string fileName)
    {
        switch (_destination)
        {
            case BackupConfiguration.BackupDestination.AmazonS3:
                _restoreSource = new DownloadFromS3(new RestoreFromS3Configuration() { Settings = _settings.S3Settings, }, database.Configuration.Backup, TaskCancelToken.Token);
                return await _restoreSource.GetStream(CombinePathAndKey(_settings.S3Settings.RemoteFolderName, folderName, fileName));
            case BackupConfiguration.BackupDestination.Azure:
                _restoreSource =  new DownloadFromAzure(new RestoreFromAzureConfiguration() { Settings = _settings.AzureSettings, }, database.Configuration.Backup, TaskCancelToken.Token);
                return await _restoreSource.GetStream(CombinePathAndKey(_settings.AzureSettings.RemoteFolderName, folderName, fileName));
            default:
                throw new ArgumentOutOfRangeException($"Missing implementation for direct upload destination '{_destination}'");
        }
    }

    public override string GetBackupDescription()
    {
        return $"{nameof(DirectBackupDownloader)}";
    }

    public void Dispose()
    {
        _restoreSource.Dispose();
        _restoreSource = null;
    }
}
