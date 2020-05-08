using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.Backups;
using Raven.Server.Documents.PeriodicBackup.Azure;
using Raven.Server.ServerWide;

namespace Raven.Server.Documents.PeriodicBackup.Restore
{
    public class RestoreFromAzure : RestoreBackupTaskBase
    {
        private readonly RavenAzureClient _client;
        private readonly string _remoteFolderName;
        public RestoreFromAzure(ServerStore serverStore, RestoreFromAzureConfiguration restoreFromConfiguration, string nodeTag, OperationCancelToken operationCancelToken) : base(serverStore, restoreFromConfiguration, nodeTag, operationCancelToken)
        {
            _client = new RavenAzureClient(restoreFromConfiguration.Settings);
            _remoteFolderName = restoreFromConfiguration.Settings.RemoteFolderName;
        }

        protected override async Task<Stream> GetStream(string path)
        {
            var blob = await _client.GetBlobAsync(path);
            return blob.Data;
        }

        protected override async Task<ZipArchive> GetZipArchiveForSnapshot(string path)
        {
            var blob = await _client.GetBlobAsync(path);
            return new ZipArchive(blob.Data, ZipArchiveMode.Read);
        }

        protected override Task<ZipArchive> GetZipArchiveForSnapshotCalc(string path)
        {
            return GetZipArchiveForSnapshot(path);
        }

        protected override async Task<List<string>> GetFilesForRestore()
        {
            var prefix = string.IsNullOrEmpty(_remoteFolderName) ? "" : _remoteFolderName;
            var allObjects = await _client.ListBlobsAsync(prefix, string.Empty, false);
            return allObjects.List.Select(x => x.Name).ToList();
        }

        protected override string GetBackupPath(string fileName)
        {
            return fileName;
        }

        protected override string GetSmugglerBackupPath(string smugglerFile)
        {
            return smugglerFile;
        }

        protected override string GetBackupLocation()
        {
            return _remoteFolderName;
        }
        protected override void Dispose()
        {
            using (_client)
            {
                base.Dispose();
            }
        }
    }
}
