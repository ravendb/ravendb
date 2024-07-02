using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.Backups;
using Raven.Server.Documents.PeriodicBackup.Azure;
using Raven.Server.ServerWide;
using Raven.Server.Utils;

namespace Raven.Server.Documents.PeriodicBackup.Restore
{
    public class RestoreFromAzure : RestoreBackupTaskBase
    {
        private readonly IRavenAzureClient _client;
        private readonly string _remoteFolderName;

        public RestoreFromAzure(ServerStore serverStore, RestoreFromAzureConfiguration restoreFromConfiguration, string nodeTag, OperationCancelToken operationCancelToken) : base(serverStore, restoreFromConfiguration, nodeTag, operationCancelToken)
        {
            _client = RavenAzureClient.Create(restoreFromConfiguration.Settings, serverStore.Configuration.Backup);
            _remoteFolderName = restoreFromConfiguration.Settings.RemoteFolderName;
        }

        protected override async Task<Stream> GetStream(string path)
        {
            var blob = await _client.GetBlobAsync(path);
            return blob.Data;
        }

        protected override async Task<ZipArchive> GetZipArchiveForSnapshot(string path, Action<string> onProgress)
        {
            var blob = await _client.GetBlobAsync(path);
            var file = await CopyRemoteStreamLocallyAsync(blob.Data, blob.Size, onProgress);
            return new DeleteOnCloseZipArchive(file, ZipArchiveMode.Read);
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
