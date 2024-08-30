using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Backups;

namespace Raven.Server.Documents.PeriodicBackup.Restore
{
    public sealed class RestoreFromLocal : IRestoreSource
    {
        private readonly string _backupLocation;

        public RestoreFromLocal(RestoreBackupConfiguration restoreConfiguration)
        {
            if (restoreConfiguration.ShardRestoreSettings != null)
                return;

            if (string.IsNullOrWhiteSpace(restoreConfiguration.BackupLocation))
                throw new ArgumentException("Backup location can't be null or empty");

            if (Directory.Exists(restoreConfiguration.BackupLocation) == false)
                throw new ArgumentException($"Backup location doesn't exist, path: {restoreConfiguration.BackupLocation}");

            _backupLocation = restoreConfiguration.BackupLocation;
        }

        public Task<Stream> GetStream(string path)
        {
            var stream = File.OpenRead(path);
            return Task.FromResult<Stream>(stream);
        }

        public Task<ZipArchive> GetZipArchiveForSnapshot(string path, Action<string> onProgress)
        {
            return Task.FromResult(ZipFile.Open(path, ZipArchiveMode.Read, System.Text.Encoding.UTF8));
        }

        public Task<List<string>> GetFilesForRestore()
        {
            return Task.FromResult(Directory.GetFiles(_backupLocation).ToList());
        }

        public string GetBackupPath(string fileName)
        {
            return fileName;
        }

        public string GetBackupLocation()
        {
            return _backupLocation;
        }

        public void Dispose()
        {
        }
    }
}
