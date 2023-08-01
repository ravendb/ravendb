using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations.Backups;
using Raven.Server.Documents.PeriodicBackup.Aws;
using Raven.Server.ServerWide;
using Raven.Server.Utils;

namespace Raven.Server.Documents.PeriodicBackup.Restore
{
    public sealed class RestoreFromS3 : IRestoreSource
    {
        private readonly ServerStore _serverStore;
        private readonly RavenAwsS3Client _client;
        private readonly string _remoteFolderName;

        public RestoreFromS3([NotNull] ServerStore serverStore, RestoreFromS3Configuration restoreFromConfiguration)
        {
            _serverStore = serverStore ?? throw new ArgumentNullException(nameof(serverStore));
            _client = new RavenAwsS3Client(restoreFromConfiguration.Settings, serverStore.Configuration.Backup);
            _remoteFolderName = restoreFromConfiguration.Settings.RemoteFolderName;
        }

        public async Task<Stream> GetStream(string path)
        {
            var blob = await _client.GetObjectAsync(path);
            return blob.Data;
        }

        public async Task<ZipArchive> GetZipArchiveForSnapshot(string path)
        {
            var blob = await _client.GetObjectAsync(path);
            var file = await RestoreUtils.CopyRemoteStreamLocallyAsync(blob.Data, _serverStore.Configuration);
            return new DeleteOnCloseZipArchive(file, ZipArchiveMode.Read);
        }

        public async Task<List<string>> GetFilesForRestore()
        {
            var prefix = string.IsNullOrEmpty(_remoteFolderName) ? "" : _remoteFolderName.TrimEnd('/') + "/";
            var allObjects = await _client.ListAllObjectsAsync(prefix, string.Empty, false);
            return allObjects.Select(x => x.FullPath).ToList();
        }

        public string GetBackupPath(string fileName)
        {
            return fileName;
        }

        public string GetSmugglerBackupPath(string smugglerFile)
        {
            return smugglerFile;
        }

        public string GetBackupLocation()
        {
            return _remoteFolderName;
        }

        public void Dispose()
        {
            _client?.Dispose();
        }
    }
}
