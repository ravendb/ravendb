using System;
using System.IO;

namespace Raven.Server.Documents.PeriodicBackup.DirectUpload;


public struct BackupDestinationStream : IDisposable
{
    public Stream Stream;
    public DirectBackupUploader BackupUploader;

    public void Dispose()
    {
        BackupUploader?.Dispose();
        Stream?.Dispose();
    }
}
