using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.Backups;
using Raven.Server.Config.Settings;
using Raven.Server.Documents.PeriodicBackup.Azure;
using Raven.Server.ServerWide;
using Raven.Server.Utils;

namespace Raven.Server.Documents.PeriodicBackup.Restore
{
    public class RestoreFromAzure : IRestoreSource
    {
        private readonly IRavenAzureClient _client;
        private readonly string _remoteFolderName;
        private readonly PathSetting _tempPath;

        public RestoreFromAzure(ServerStore serverStore, RestoreFromAzureConfiguration restoreFromConfiguration)
        {
            _client = RavenAzureClient.Create(restoreFromConfiguration.Settings, serverStore.Configuration.Backup);
            _remoteFolderName = restoreFromConfiguration.Settings.RemoteFolderName;
            _tempPath = serverStore.Configuration.Storage.TempPath;
        }

        public async Task<Stream> GetStream(string path)
        {
            var blob = await _client.GetBlobAsync(path);
            return blob.Data;
        }

        public async Task<ZipArchive> GetZipArchiveForSnapshot(string path)
        {
            var blob = await _client.GetBlobAsync(path);
            var file = await RestoreUtils.CopyRemoteStreamLocally(blob.Data, _tempPath);
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
