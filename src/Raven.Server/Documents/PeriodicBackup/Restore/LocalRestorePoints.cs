using System;
using System.Collections.Generic;
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
            var filesInfo = new List<FileInfoDetails>();

            foreach (var filePath in Directory.GetFiles(directoryPath))
            {
                if (TryExtractDateFromFileName(filePath, out var lastModified) == false)
                    lastModified = File.GetLastWriteTimeUtc(filePath).ToLocalTime();

                filesInfo.Add(new FileInfoDetails(filePath, Path.GetDirectoryName(filePath), lastModified));
            }

            return Task.FromResult(filesInfo);
        }

        protected override ParsedBackupFolderName ParseFolderNameFrom(string path)
        {
            var lastFolderName = Path.GetFileName(path);
            return ParseFolderName(lastFolderName);
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
    }
}
