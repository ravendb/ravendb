using System;
using System.Collections.Generic;
using System.IO.Compression;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.Backups;
using Raven.Server.Documents.PeriodicBackup.Aws;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.PeriodicBackup.Restore
{
    public class S3RestorePoints : RestorePointsBase
    {
        private readonly RavenAwsS3Client _client;

        public S3RestorePoints(SortedList<DateTime, RestorePoint> sortedList, TransactionOperationContext context, S3Settings s3Settings) : base(sortedList, context)
        {
            _client = new RavenAwsS3Client(s3Settings);
        }

        public override async Task FetchRestorePoints(string path)
        {
            path = path.TrimEnd('/');

            var files = await _client.ListAllObjects(path + "/", string.Empty, true);
            var folders = files.Select(x => GetFolderName(x.FullPath));

            foreach (var folder in folders)
            {
                await FetchRestorePointsForPath(folder, assertLegacyBackups: true);
            }
        }

        protected override async Task<List<FileInfoDetails>> GetFiles(string path)
        {
            path = path.TrimEnd('/');

            var allObjects = await _client.ListAllObjects(path + "/", string.Empty, false);

            foreach (var obj in allObjects)
            {
                if (TryExtractDateFromFileName(obj.FullPath, out var lastModified))
                    obj.LastModified = lastModified;
            }

            return allObjects;
        }

        protected override ParsedBackupFolderName ParseFolderNameFrom(string path)
        {
            var arr = path.Split('/', StringSplitOptions.RemoveEmptyEntries);
            var lastFolderName = arr.Length > 0 ? arr[arr.Length - 1] : string.Empty;

            return ParseFolderName(lastFolderName);
        }

        protected override async Task<ZipArchive> GetZipArchive(string filePath)
        {
            var blob = await _client.GetObject(filePath);
            return new ZipArchive(blob.Data, ZipArchiveMode.Read);
        }

        protected override string GetFileName(string fullPath)
        {
            return fullPath.Split('/').Last();
        }

        public override void Dispose()
        {
            _client.Dispose();
        }

        private static string GetFolderName(string fullPath)
        {
            return fullPath.Replace(fullPath.Substring(fullPath.LastIndexOf('/')), "");
        }
    }
}
