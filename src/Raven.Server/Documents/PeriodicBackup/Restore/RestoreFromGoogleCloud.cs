using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.Backups;
using Raven.Server.Documents.Indexes.Static.Extensions;
using Raven.Server.Documents.PeriodicBackup.GoogleCloud;
using Raven.Server.ServerWide;

namespace Raven.Server.Documents.PeriodicBackup.Restore
{
    class RestoreFromGoogleCloud : RestoreBackupTaskBase
    {
        private readonly RavenGoogleCloudClient _client;
        private readonly string _remoteFolderName;

        public RestoreFromGoogleCloud(ServerStore serverStore,RestoreFromGoogleCloudConfiguration restoreFromConfiguration, string nodeTag, OperationCancelToken operationCancelToken) : base(serverStore, restoreFromConfiguration, nodeTag, operationCancelToken)
        {
            _client = new RavenGoogleCloudClient(restoreFromConfiguration.Settings);
            _remoteFolderName = restoreFromConfiguration.Settings.RemoteFolderName;
        }

        protected override async Task<Stream> GetStream(string path)
        {
            var obj = new MemoryStream();
            await _client.DownloadObjectAsync(path, obj);
            obj.Position = 0;
            return obj;
        }

        protected override async Task<ZipArchive> GetZipArchiveForSnapshot(string path)
        {
            var obj = new MemoryStream();
            await _client.DownloadObjectAsync(path, obj);
            obj.Position = 0;
            return new ZipArchive(obj, ZipArchiveMode.Read);
        }

        protected override Task<ZipArchive> GetZipArchiveForSnapshotCalc(string path)
        {
            return GetZipArchiveForSnapshot(path);
        }

        protected override async Task<List<string>> GetFilesForRestore()
        {
            var prefix = string.IsNullOrEmpty(_remoteFolderName) ? "" : _remoteFolderName.TrimEnd('/');
            var allObjects = await _client.ListObjectsAsync(prefix, null);
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
