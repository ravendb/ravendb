using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.Backups;

namespace Raven.Server.Documents.PeriodicBackup.Restore
{
    public sealed class RestoreFromLocal : IRestoreSource
    {
        public string _remoteFolderName { get; set; }

        public RestoreFromLocal(RestoreBackupConfiguration restoreConfiguration)
        {
            if (restoreConfiguration.ShardRestoreSettings != null)
                return;

            if (string.IsNullOrWhiteSpace(restoreConfiguration.BackupLocation))
                throw new ArgumentException("Backup location can't be null or empty");

            if (Directory.Exists(restoreConfiguration.BackupLocation) == false)
                throw new ArgumentException($"Backup location doesn't exist, path: {restoreConfiguration.BackupLocation}");

            _remoteFolderName = restoreConfiguration.BackupLocation;
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
            return Task.FromResult(Directory.GetFiles(_remoteFolderName).ToList());
        }

        public Task ValidateConfigurationsAsync()
        {
            return Task.CompletedTask;
        }

        public void Dispose()
        {
        }
    }
}
