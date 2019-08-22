using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.Backups;
using Raven.Server.Documents.PeriodicBackup.GoogleCloud;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.PeriodicBackup.Restore
{
    class GoogleCloudRestorePoints : RestorePointsBase
    {
        private readonly RavenGoogleCloudClient _client;

        public GoogleCloudRestorePoints(SortedList<DateTime, RestorePoint> sortedList, TransactionOperationContext context, GoogleCloudSettings client) : base(sortedList, context)
        {
            _client = new RavenGoogleCloudClient(client);
        }

        public override async Task FetchRestorePoints(string path)
        {
            var objects = await _client.ListObjectsAsync(path, "/");

            await FetchRestorePointsForPath(path, assertLegacyBackups: true);

        }

        protected override async Task<List<FileInfoDetails>> GetFiles(string path)
        {
            var objects = await _client.ListObjectsAsync(path, "/");

            var filesInfo = new List<FileInfoDetails>();

            foreach (var obj in objects)
            {
                if (TryExtractDateFromFileName(obj.Name, out var lastModified) == false)
                    lastModified = Convert.ToDateTime(obj.Updated);

                filesInfo.Add(new FileInfoDetails
                {
                    FullPath = obj.Name,
                    LastModified = lastModified
                });
            }

            return filesInfo;
        }

        protected override ParsedBackupFolderName ParseFolderNameFrom(string path)
        {
            var arr = path.Split('/', StringSplitOptions.RemoveEmptyEntries);
            var lastFolderName = arr.Length > 0 ? arr[arr.Length - 1] : string.Empty;

            return ParseFolderName(lastFolderName);
        }

        protected override async Task<ZipArchive> GetZipArchive(string filePath)
        {
            var file = new MemoryStream();
            await _client.DownloadObjectAsync(filePath, file);
            return new ZipArchive(file, ZipArchiveMode.Read);
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
