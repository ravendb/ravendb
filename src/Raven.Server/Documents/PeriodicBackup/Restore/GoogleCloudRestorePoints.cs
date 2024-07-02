using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.Backups;
using Raven.Server.Config;
using Raven.Server.Documents.PeriodicBackup.GoogleCloud;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;

namespace Raven.Server.Documents.PeriodicBackup.Restore
{
    public sealed class GoogleCloudRestorePoints : RestorePointsBase
    {
        private readonly RavenConfiguration _configuration;
        private readonly CancellationToken _cancellationToken;
        private readonly RavenGoogleCloudClient _client;

        public GoogleCloudRestorePoints(RavenConfiguration configuration, TransactionOperationContext context, GoogleCloudSettings client, CancellationToken cancellationToken) : base(context)
        {
            _configuration = configuration;
            _cancellationToken = cancellationToken;
            _client = new RavenGoogleCloudClient(client, configuration.Backup, cancellationToken: cancellationToken);
        }

        public override Task<RestorePoints> FetchRestorePoints(string path, int? shardNumber = null)
        {
            return FetchRestorePointsForPath(path, assertLegacyBackups: true, shardNumber);
        }

        protected override async Task<List<FileInfoDetails>> GetFiles(string path)
        {
            var objects = await _client.ListObjectsAsync(path, delimiter: null);

            var filesInfo = new List<FileInfoDetails>();

            foreach (var obj in objects)
            {
                if (TryExtractDateFromFileName(obj.Name, out var lastModified) == false)
                    lastModified = obj.UpdatedDateTimeOffset?.DateTime ?? DateTime.Now;

                var fullPath = obj.Name;
                var directoryPath = GetDirectoryName(fullPath);
                filesInfo.Add(new FileInfoDetails(fullPath, directoryPath, lastModified));
            }

            return filesInfo;
        }

        protected override ParsedBackupFolderName ParseFolderNameFrom(string path)
        {
            var arr = path.Split('/', StringSplitOptions.RemoveEmptyEntries);
            var lastFolderName = arr.Length > 0 ? arr[^1] : string.Empty;

            return ParseFolderName(lastFolderName);
        }

        protected override async Task<ZipArchive> GetZipArchive(string filePath)
        {
            Stream downloadObject = _client.DownloadObject(filePath);
            var size = await _client.GetObjectSizeAsync(filePath);
            var file = await RestoreUtils.CopyRemoteStreamLocallyAsync(downloadObject, size, _configuration, _cancellationToken);
            return new DeleteOnCloseZipArchive(file, ZipArchiveMode.Read);
        }

        protected override string GetFileName(string fullPath)
        {
            return fullPath.Split('/').Last();
        }

        public override void Dispose()
        {
            _client.Dispose();
        }
    }
}
