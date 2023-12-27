using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.Backups;
using Raven.Server.Documents.PeriodicBackup.GoogleCloud;
using Raven.Server.ServerWide;
using Raven.Server.Utils;
using Sparrow.Server.Utils;

namespace Raven.Server.Documents.PeriodicBackup.Restore
{
    public class RestoreFromGoogleCloud : RestoreBackupTaskBase
    {
        private readonly RavenGoogleCloudClient _client;
        private readonly string _remoteFolderName;

        public RestoreFromGoogleCloud(ServerStore serverStore, RestoreFromGoogleCloudConfiguration restoreFromConfiguration, string nodeTag, OperationCancelToken operationCancelToken) : base(serverStore, restoreFromConfiguration, nodeTag, operationCancelToken)
        {
            _client = new RavenGoogleCloudClient(restoreFromConfiguration.Settings, serverStore.Configuration.Backup);
            _remoteFolderName = restoreFromConfiguration.Settings.RemoteFolderName;
        }

        protected override Task<Stream> GetStream(string path)
        {
            return Task.FromResult(_client.DownloadObject(path));
        }

        protected override async Task<ZipArchive> GetZipArchiveForSnapshot(string path)
        {
            Stream stream = _client.DownloadObject(path);
            var file = await CopyRemoteStreamLocally(stream);
            return new DeleteOnCloseZipArchive(file, ZipArchiveMode.Read);
        }

        protected override async Task<List<string>> GetFilesForRestore()
        {
            var prefix = string.IsNullOrEmpty(_remoteFolderName) ? "" : _remoteFolderName.TrimEnd('/');
            var allObjects = await _client.ListObjectsAsync(prefix, delimiter: null);
            var result = new List<string>();
            foreach (var obj in allObjects)
            {
                result.Add(obj.Name);
            }

            return result;
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
