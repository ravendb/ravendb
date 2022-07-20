using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.Backups;
using Raven.Server.Documents.PeriodicBackup.GoogleCloud;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;

namespace Raven.Server.Documents.PeriodicBackup.Restore
{
    public class GoogleCloudRestorePoints : RestorePointsBase
    {
        private readonly Config.Categories.BackupConfiguration _configuration;
        private readonly RavenGoogleCloudClient _client;

        public GoogleCloudRestorePoints(Config.Categories.BackupConfiguration configuration, SortedList<DateTime, RestorePoint> sortedList, TransactionOperationContext context, GoogleCloudSettings client) : base(sortedList, context)
        {
            _configuration = configuration;
            _client = new RavenGoogleCloudClient(client, configuration);
        }

        public override async Task FetchRestorePoints(string path)
        {
            await FetchRestorePointsForPath(path, assertLegacyBackups: true);
        }

        protected override async Task<List<FileInfoDetails>> GetFiles(string path)
        {
            var objects = await _client.ListObjectsAsync(path, delimiter: null);

            var filesInfo = new List<FileInfoDetails>();

            foreach (var obj in objects)
            {
                if (TryExtractDateFromFileName(obj.Name, out var lastModified) == false)
                    lastModified = Convert.ToDateTime(obj.Updated);

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
            var file = await RestoreUtils.CopyRemoteStreamLocally(downloadObject, _configuration.TempPath);
            return new DeleteOnCloseZipArchive(downloadObject, ZipArchiveMode.Read);
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
