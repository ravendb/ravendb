using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.Backups;
using Raven.Server.Documents.PeriodicBackup.Aws;
using Raven.Server.ServerWide;

namespace Raven.Server.Documents.PeriodicBackup.Restore
{
    public class RestoreFromS3 : RestoreBackupTaskBase
    {
        private readonly RavenAwsS3Client _client;
        private readonly string _remoteFolderName;

        public RestoreFromS3(ServerStore serverStore, RestoreFromS3Configuration restoreFromConfiguration, string nodeTag, OperationCancelToken operationCancelToken) : base(serverStore, restoreFromConfiguration, nodeTag, operationCancelToken)
        {
            _client = new RavenAwsS3Client(restoreFromConfiguration.Settings);
            _remoteFolderName = restoreFromConfiguration.Settings.RemoteFolderName;
        }

        protected override async Task<Stream> GetStream(string path)
        {
            var blob = await _client.GetObject(path);
            return blob.Data;
        }

        protected override async Task<ZipArchive> GetZipArchiveForSnapshot(string path)
        {
            var blob = await _client.GetObject(path);
            return new ZipArchive(blob.Data, ZipArchiveMode.Read);
        }

        protected override async Task<List<string>> GetFilesForRestore()
        {
            var files = await _client.ListObjects(string.IsNullOrEmpty(_remoteFolderName) ? "" : _remoteFolderName + "/", "/", false);
            return files.Select(x => x.FullPath).ToList();
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
