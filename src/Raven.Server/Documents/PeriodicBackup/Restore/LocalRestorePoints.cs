using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Threading.Tasks;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.PeriodicBackup.Restore
{
    public class LocalRestorePoints : RestorePointsBase
    {
        public LocalRestorePoints(SortedList<DateTime, RestorePoint> sortedList, TransactionOperationContext context) : base(sortedList, context)
        {
        }

        public override async Task FetchRestorePoints(string directoryPath)
        {
            var directories = Directory.GetDirectories(directoryPath).OrderBy(x => x).ToList();
            if (directories.Count == 0)
            {
                // no folders in directory
                // will scan the directory for backup files
                await FetchRestorePointsForPath(directoryPath, assertLegacyBackups: true);
            }
            else
            {
                foreach (var directory in directories)
                {
                    await FetchRestorePointsForPath(directory, assertLegacyBackups: true);
                }
            }
        }

        protected override Task<List<FileInfoDetails>> GetFiles(string directoryPath)
        {
            var fileInfos = new List<FileInfoDetails>();

            foreach (var filePath in Directory.GetFiles(directoryPath))
            {
                fileInfos.Add(new FileInfoDetails
                {
                    FullPath = filePath,
                    LastModified = ExtractDateFromFileName(filePath)
                });
            }

            return Task.FromResult(fileInfos);
        }

        protected override (string DatabaseName, string NodeTag) ParseFolderName(string lastFolderName)
        {
            // [Date].ravendb-[Database Name]-[Node Tag]-[Backup Type]
            // [DATE] - format: "yyyy-MM-dd-HH-mm"
            // [Backup Type] - backup/snapshot
            // example: //2018-02-03-15-34.ravendb-Northwind-A-backup

            var match = BackupFolderRegex.Match(lastFolderName);
            return match.Success
                ? (match.Groups[1].Value, match.Groups[2].Value)
                : (null, null);
        }

        protected override Task<ZipArchive> GetZipArchive(string filePath)
        {
            return Task.FromResult(ZipFile.Open(filePath, ZipArchiveMode.Read, System.Text.Encoding.UTF8));
        }

        protected override string GetFileName(string fullPath)
        {
            return Path.GetFileName(fullPath);
        }

        public override void Dispose()
        {            
        }

        private static DateTime ExtractDateFromFileName(string filePath)
        {
            // file name format: 2017-06-01-00-00-00
            // legacy incremental backup format: 2017-06-01-00-00-00-0

            var fileNameWithoutExtension = Path.GetFileNameWithoutExtension(filePath);
            var match = FileNameRegex.Match(fileNameWithoutExtension);
            if (match.Success)
            {
                fileNameWithoutExtension = match.Value;
            }

            if (DateTime.TryParseExact(
                    fileNameWithoutExtension,
                    BackupTask.DateTimeFormat,
                    CultureInfo.InvariantCulture,
                    DateTimeStyles.None,
                    out DateTime result) == false)
            {
                result = File.GetLastWriteTimeUtc(filePath).ToLocalTime();
            }

            return result;
        }
    }
}
