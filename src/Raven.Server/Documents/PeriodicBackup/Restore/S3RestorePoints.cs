using System;
using System.Collections.Generic;
using System.IO.Compression;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.Backups;
using Raven.Server.Config;
using Raven.Server.Documents.PeriodicBackup.Aws;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;

namespace Raven.Server.Documents.PeriodicBackup.Restore
{
    public sealed class S3RestorePoints : RestorePointsBase
    {
        private readonly RavenConfiguration _configuration;
        private readonly CancellationToken _cancellationToken;
        private readonly RavenAwsS3Client _client;

        public S3RestorePoints(RavenConfiguration configuration, TransactionOperationContext context, S3Settings s3Settings, CancellationToken cancellationToken) : base(context)
        {
            _configuration = configuration;
            _cancellationToken = cancellationToken;
            _client = new RavenAwsS3Client(s3Settings, configuration.Backup, cancellationToken: cancellationToken);
        }

        public override async Task<RestorePoints> FetchRestorePoints(string path, int? shardNumber = null)
        {
            path = path.TrimEnd('/');
            var objects = await _client.ListAllObjectsAsync(string.IsNullOrEmpty(path) ? "" : path + "/", "/", listFolders: true);
            var folders = objects.Select(x => x.FullPath).ToList();

            if (folders.Count == 0)
            {
                return await FetchRestorePointsForPath(path, assertLegacyBackups: true, shardNumber);
            }

            return await FetchRestorePointsForPaths(folders, assertLegacyBackups: true, shardNumber);

        }

        protected override async Task<List<FileInfoDetails>> GetFiles(string path)
        {
            path = path.TrimEnd('/');

            var allObjects = await _client.ListAllObjectsAsync(path + "/", string.Empty, false);

            var filesInfo = new List<FileInfoDetails>();

            foreach (var obj in allObjects)
            {
                if (TryExtractDateFromFileName(obj.FullPath, out var lastModified) == false)
                    lastModified = obj.LastModified;

                var fullPath = obj.FullPath;
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
            var blob = await _client.GetObjectAsync(filePath);
            var file = await RestoreUtils.CopyRemoteStreamLocallyAsync(blob.Data, blob.Size, _configuration, onProgress: null, _cancellationToken);
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
