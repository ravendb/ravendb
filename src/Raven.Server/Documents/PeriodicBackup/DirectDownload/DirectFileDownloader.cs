using System;
using System.IO;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.Backups;
using Raven.Server.Documents.PeriodicBackup.Restore;
using Raven.Server.ServerWide;

namespace Raven.Server.Documents.PeriodicBackup.DirectDownload;

public sealed class DirectFileDownloader : FileUploaderDownloaderBase, IDisposable
{
    private IRestoreSource _restoreSource;
    public DirectFileDownloader(UploaderSettings settings, OperationCancelToken taskCancelToken) : base(settings, taskCancelToken)
    {
    }

    internal Task<Stream> StreamForDownloadDestination(DocumentDatabase database, string folderName, string fileName)
    {
        switch (_destination)
        {
            case BackupConfiguration.BackupDestination.AmazonS3:
                _restoreSource = new DownloadFromS3(new RestoreFromS3Configuration() { Settings = _settings.S3Settings, }, database.Configuration.Backup, TaskCancelToken.Token);
                return _restoreSource.GetStream(CombinePathAndKey(_settings.S3Settings.RemoteFolderName, folderName, fileName));
            case BackupConfiguration.BackupDestination.Azure:
                _restoreSource = new DownloadFromAzure(new RestoreFromAzureConfiguration() { Settings = _settings.AzureSettings, }, database.Configuration.Backup, TaskCancelToken.Token);
                return _restoreSource.GetStream(CombinePathAndKey(_settings.AzureSettings.RemoteFolderName, folderName, fileName));
            default:
                throw new ArgumentOutOfRangeException($"Missing implementation for direct upload destination '{_destination}'");
        }
    }

    public void Dispose()
    {
        _restoreSource?.Dispose();
        _restoreSource = null;
    }
}
