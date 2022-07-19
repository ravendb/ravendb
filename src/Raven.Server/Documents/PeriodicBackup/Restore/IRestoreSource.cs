using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Threading.Tasks;

namespace Raven.Server.Documents.PeriodicBackup.Restore
{
    public interface IRestoreSource : IDisposable
    {
        Task<Stream> GetStream(string path);

        Task<ZipArchive> GetZipArchiveForSnapshot(string path);

        Task<List<string>> GetFilesForRestore();

        string GetBackupPath(string smugglerFile);

        string GetSmugglerBackupPath(string smugglerFile);

        string GetBackupLocation();

    }
}
