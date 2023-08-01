using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations.Backups;
using Raven.Server.Documents.PeriodicBackup.Azure;
using Raven.Server.ServerWide;
using Raven.Server.Utils;

namespace Raven.Server.Documents.PeriodicBackup.Restore
{
    public sealed class RestoreFromAzure : IRestoreSource
    {
        private readonly ServerStore _serverStore;
        private readonly IRavenAzureClient _client;
        private readonly string _remoteFolderName;

        public RestoreFromAzure([NotNull] ServerStore serverStore, RestoreFromAzureConfiguration restoreFromConfiguration)
        {
            _serverStore = serverStore ?? throw new ArgumentNullException(nameof(serverStore));
            _client = RavenAzureClient.Create(restoreFromConfiguration.Settings, serverStore.Configuration.Backup);
            _remoteFolderName = restoreFromConfiguration.Settings.RemoteFolderName;
        }

        public async Task<Stream> GetStream(string path)
        {
            var blob = await _client.GetBlobAsync(path);
            return blob.Data;
        }

        public async Task<ZipArchive> GetZipArchiveForSnapshot(string path)
        {
            var blob = await _client.GetBlobAsync(path);
            var file = await RestoreUtils.CopyRemoteStreamLocallyAsync(blob.Data, _serverStore.Configuration);
            return new DeleteOnCloseZipArchive(file, ZipArchiveMode.Read);
        }

        public async Task<List<string>> GetFilesForRestore()
        {
            var prefix = string.IsNullOrEmpty(_remoteFolderName) ? "" : _remoteFolderName;
            var allObjects = await _client.ListBlobsAsync(prefix, string.Empty, false);
            return allObjects.List.Select(x => x.Name).ToList();
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
