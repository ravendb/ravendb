using System;
using System.IO;

namespace Raven.Server.Documents.PeriodicBackup.DirectUpload;


public struct BackupDestinationStream : IDisposable
{
    public Stream Stream;
    public DirectFileUploader FileUploader;

    public void Dispose()
    {
        FileUploader?.Reset();
        Stream?.Dispose();
    }
}
