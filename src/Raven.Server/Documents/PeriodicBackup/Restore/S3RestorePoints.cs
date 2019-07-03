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
            
            var files = await _client.ListObjects(path + "/", string.Empty, false);
            var folders = files.GroupBy(x => GetFolderName(x.FullPath));

            foreach (var folder in folders)
            {
                await FetchRestorePointsForPath(folder.Key, assertLegacyBackups: true);
            }
        }

        protected override async Task<List<FileInfoDetails>> GetFiles(string path)
        {
            var files = await _client.ListObjects(path, string.Empty, false);
            return files.ToList();
        }

        protected override (string DatabaseName, string NodeTag) ParseFolderName(string path)
        {
            // [Date].ravendb-[Database Name]-[Node Tag]-[Backup Type]
            // [DATE] - format: "yyyy-MM-dd-HH-mm"
            // [Backup Type] - backup/snapshot
            // example: //2018-02-03-15-34.ravendb-Northwind-A-backup

            var arr =  path.Split('/', StringSplitOptions.RemoveEmptyEntries);
            var lastFolderName = arr.Length > 0 ? arr[arr.Length-1] : string.Empty;

            var match = BackupFolderRegex.Match(lastFolderName);
            return match.Success
                ? (match.Groups[1].Value, match.Groups[2].Value)
                : (null, null);
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
