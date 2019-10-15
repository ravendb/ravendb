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
using Sparrow.Server.Utils;

namespace Raven.Server.Documents.PeriodicBackup.Restore
{
    public class GoogleCloudRestorePoints : RestorePointsBase
    {
        private readonly RavenGoogleCloudClient _client;

        public GoogleCloudRestorePoints(SortedList<DateTime, RestorePoint> sortedList, TransactionOperationContext context, GoogleCloudSettings client) : base(sortedList, context)
        {
            _client = new RavenGoogleCloudClient(client);
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
            var lastFolderName = arr.Length > 0 ? arr[arr.Length - 1] : string.Empty;

            return ParseFolderName(lastFolderName);
        }

        protected override Task<ZipArchive> GetZipArchive(string filePath)
        {
            return Task.FromResult(new ZipArchive(_client.DownloadObject(filePath), ZipArchiveMode.Read));
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
