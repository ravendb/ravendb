using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.Backups;
using Raven.Server.Documents.PeriodicBackup.Aws;
using Raven.Server.ServerWide;
using Raven.Server.Utils;

namespace Raven.Server.Documents.PeriodicBackup.Restore
{
    public class RestoreFromS3 : RestoreBackupTaskBase
    {
        private readonly RavenAwsS3Client _client;
        private readonly string _remoteFolderName;

        public RestoreFromS3(ServerStore serverStore, RestoreFromS3Configuration restoreFromConfiguration, string nodeTag, OperationCancelToken operationCancelToken) : base(serverStore, restoreFromConfiguration, nodeTag, operationCancelToken)
        {
            _client = new RavenAwsS3Client(restoreFromConfiguration.Settings, serverStore.Configuration.Backup);
            _remoteFolderName = restoreFromConfiguration.Settings.RemoteFolderName;
        }

        protected override async Task<Stream> GetStream(string path)
        {
            var blob = await _client.GetObjectAsync(path);
            return blob.Data;
        }

        protected override async Task<ZipArchive> GetZipArchiveForSnapshot(string path, Action<string> onProgress)
        {
            var blob = await _client.GetObjectAsync(path);
            var file = await CopyRemoteStreamLocallyAsync(blob.Data, blob.Size, onProgress);
            return new DeleteOnCloseZipArchive(file, ZipArchiveMode.Read);
        }

        protected override async Task<List<string>> GetFilesForRestore()
        {
            var prefix = string.IsNullOrEmpty(_remoteFolderName) ? "" : _remoteFolderName.TrimEnd('/') + "/";
            var allObjects = await _client.ListAllObjectsAsync(prefix, string.Empty, false);
            return allObjects.Select(x => x.FullPath).ToList();
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
