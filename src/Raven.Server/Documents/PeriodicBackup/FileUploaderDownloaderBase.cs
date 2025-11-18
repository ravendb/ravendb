using System;
using Raven.Client.Documents.Operations.Backups;
using Raven.Server.ServerWide;

namespace Raven.Server.Documents.PeriodicBackup;

public abstract class FileUploaderDownloaderBase
{
    public readonly OperationCancelToken TaskCancelToken;
    internal readonly BackupConfiguration.BackupDestination _destination;
    protected readonly UploaderSettings _settings;

    protected FileUploaderDownloaderBase(UploaderSettings settings, OperationCancelToken taskCancelToken)
    {
        _settings = settings ?? throw new ArgumentNullException(nameof(settings));
        _destination = settings.Destination;
        TaskCancelToken = taskCancelToken;
    }

    protected string CombinePathAndKey(string path)
    {
        return CombinePathAndKey(path, _settings.FolderName, _settings.FileName);
    }

    protected string CombinePathAndKey(string path, string folderName, string fileName)
    {
        if (path?.EndsWith('/') == true)
            path = path[..^1];

        var prefix = string.IsNullOrWhiteSpace(path) == false ? $"{path}/" : string.Empty;
        prefix = string.IsNullOrWhiteSpace(folderName) == false ? $"{prefix}{folderName}/" : prefix;

        return $"{prefix}{fileName}";
    }
}
