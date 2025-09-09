using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Threading.Tasks;

namespace Raven.Server.Documents.PeriodicBackup.Restore
{
    public interface IDownloadSource : IDisposable
    {
        Task<Stream> GetStream(string path);
    }

    public interface IRestoreSource : IDownloadSource
    {
        string _remoteFolderName { get; set; }

        Task<ZipArchive> GetZipArchiveForSnapshot(string path, Action<string> onProgress);

        Task<List<string>> GetFilesForRestore();

        Task ValidateConfigurationsAsync();

        public string GetBackupPath(string fileName)
        {
            return fileName;
        }

        public string GetBackupLocation()
        {
            return _remoteFolderName;
        }
    }
}
