using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.Backups;
using Raven.Server.ServerWide;
using Voron.Util.Settings;

namespace Raven.Server.Documents.PeriodicBackup.Restore
{
    public class RestoreFromLocal : RestoreBackupTaskBase
    {
        private readonly string _backupLocation;

        public RestoreFromLocal(ServerStore serverStore, RestoreBackupConfiguration restoreConfiguration, string nodeTag, OperationCancelToken operationCancelToken) : base(serverStore, restoreConfiguration, nodeTag, operationCancelToken)
        {
            if (string.IsNullOrWhiteSpace(restoreConfiguration.BackupLocation))
                throw new ArgumentException("Backup location can't be null or empty");

            if (Directory.Exists(restoreConfiguration.BackupLocation) == false)
                throw new ArgumentException($"Backup location doesn't exist, path: {_backupLocation}");

            _backupLocation = restoreConfiguration.BackupLocation;
        }

        protected override Task<Stream> GetStream(string path)
        {
            var stream = File.OpenRead(path);
            return Task.FromResult<Stream>(stream);
        }

        protected override Task<ZipArchive> GetZipArchiveForSnapshot(string path)
        {
            return Task.FromResult(ZipFile.Open(path, ZipArchiveMode.Read, System.Text.Encoding.UTF8));
        }

        protected override Task<List<string>> GetFilesForRestore()
        {
            return Task.FromResult(Directory.GetFiles(_backupLocation).ToList());
        }

        protected override string GetBackupPath(string fileName)
        {
            return fileName;
        }

        protected override string GetBackupLocation()
        {
            return _backupLocation;
        }
    }
}
