using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations;

namespace Raven.Server.Documents.PeriodicBackup.Restore
{
    public interface IRestoreSource : IDisposable
    {
        Task<Stream> GetStream(string path);

        Task<ZipArchive> GetZipArchiveForSnapshot(string path, Action<string> onProgress);

        Task<List<string>> GetFilesForRestore();

        string GetBackupPath(string smugglerFile);

        string GetBackupLocation();
    }
}
