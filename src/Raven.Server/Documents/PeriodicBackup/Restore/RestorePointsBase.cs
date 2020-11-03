using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Documents.Smuggler;
using Raven.Server.Json;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.PeriodicBackup.Restore
{
    public abstract class RestorePointsBase : IDisposable
    {
        protected static readonly Regex BackupFolderRegex = new Regex(@"([0-9]{4}-[0-9]{2}-[0-9]{2}-[0-9]{2}-[0-9]{2}(-[0-9]{2})?).ravendb-(.+)-([A-Za-z]+)-(.+)$", RegexOptions.Compiled);
        protected static readonly Regex FileNameRegex = new Regex(@"([0-9]{4}-[0-9]{2}-[0-9]{2}-[0-9]{2}-[0-9]{2}(-[0-9]{2})?)", RegexOptions.Compiled);

        private readonly SortedList<DateTime, RestorePoint> _sortedList;
        private readonly TransactionOperationContext _context;

        protected RestorePointsBase(SortedList<DateTime, RestorePoint> sortedList, TransactionOperationContext context)
        {
            _sortedList = sortedList;
            _context = context;
        }

        public abstract Task FetchRestorePoints(string path);

        protected abstract Task<List<FileInfoDetails>> GetFiles(string path);

        protected abstract ParsedBackupFolderName ParseFolderNameFrom(string path);

        protected abstract Task<ZipArchive> GetZipArchive(string filePath);

        protected abstract string GetFileName(string fullPath);

        public abstract void Dispose();

        public static ParsedBackupFolderName ParseFolderName(string folderName)
        {
            // [Date].ravendb-[Database Name]-[Node Tag]-[Backup Type]
            // [DATE] - format: "yyyy-MM-dd-HH-mm"
            // [Backup Type] - backup/snapshot
            // example 1: //2018-02-03-15-34.ravendb-Northwind-A-backup
            // example 2: //2018-02-03-15-34-02.ravendb-Northwind-A-backup

            var match = BackupFolderRegex.Match(folderName);
            if (match.Success)
            {
                return new ParsedBackupFolderName
                {
                    BackupTimeAsString = match.Groups[1].Value,
                    DatabaseName = match.Groups[3].Value,
                    NodeTag = match.Groups[4].Value
                };
            }

            return new ParsedBackupFolderName();
        }

        public static bool TryExtractDateFromFileName(string filePath, out DateTime lastModified)
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
                    BackupTask.GetDateTimeFormat(fileNameWithoutExtension),
                    CultureInfo.InvariantCulture,
                    DateTimeStyles.None,
                    out lastModified) == false)
            {
                return false;
            }

            return true;
        }

        public class ParsedBackupFolderName
        {
            public string BackupTimeAsString { get; set; }

            public string DatabaseName { get; set; }

            public string NodeTag { get; set; }
        }

        protected async Task FetchRestorePointsForPath(string path, bool assertLegacyBackups)
        {
            var fileInfos = (await GetFiles(path))
                .Where(filePath =>
                {
                    if (assertLegacyBackups)
                    {
                        const string legacyEsentBackupFile = "RavenDB.Backup";
                        const string legacyVoronBackupFile = "RavenDB.Voron.Backup";

                        var fileName = filePath.FullPath;
                        if (fileName.Equals(legacyEsentBackupFile, StringComparison.OrdinalIgnoreCase) ||
                            fileName.Equals(legacyVoronBackupFile, StringComparison.OrdinalIgnoreCase))
                        {
                            throw new InvalidOperationException("Cannot restore a legacy backup (v3.x and below). " +
                                                                "You can restore a v3.x periodic export backup or " +
                                                                "use an export from v3.x and import it using the studio");
                        }
                    }

                    return IsBackupOrSnapshot(filePath.FullPath);
                })
                .OrderBy(x => Path.GetFileNameWithoutExtension(x.FullPath))
                .ThenBy(x => Path.GetExtension(x.FullPath), PeriodicBackupFileExtensionComparer.Instance)
                .ThenBy(x => x.LastModified)
                .GroupBy(x => x.DirectoryPath);

            foreach (var fileInfo in fileInfos)
            {
                await UpdateRestorePoints(fileInfo.ToList());
            }

            foreach (var restorePointGroup in _sortedList.Values.GroupBy(x => x.Location))
            {
                var count = restorePointGroup.Count();
                foreach (var restorePoint in restorePointGroup)
                    restorePoint.FilesToRestore = count--;
            }
        }

        private async Task UpdateRestorePoints(List<FileInfoDetails> fileInfos)
        {
            var firstFile = true;
            var snapshotRestore = false;
            var isEncrypted = false;

            foreach (var fileInfo in fileInfos)
            {
                var extension = Path.GetExtension(fileInfo.FullPath);
                var isSnapshot = Constants.Documents.PeriodicBackup.SnapshotExtension.Equals(extension, StringComparison.OrdinalIgnoreCase) ||
                                 Constants.Documents.PeriodicBackup.EncryptedSnapshotExtension.Equals(extension, StringComparison.OrdinalIgnoreCase);

                if (firstFile)
                {
                    snapshotRestore = isSnapshot;
                    if (Constants.Documents.PeriodicBackup.EncryptedSnapshotExtension.Equals(extension, StringComparison.OrdinalIgnoreCase) ||
                        Constants.Documents.PeriodicBackup.EncryptedIncrementalBackupExtension.Equals(extension, StringComparison.OrdinalIgnoreCase) ||
                        Constants.Documents.PeriodicBackup.EncryptedFullBackupExtension.Equals(extension, StringComparison.OrdinalIgnoreCase))
                    {
                        isEncrypted = true;
                    }
                    else if (isSnapshot && this is LocalRestorePoints)
                    {
                        // checking legacy encrypted snapshot backups, it was saved with the same extension as the non-encrypted one
                        // local only since this might require the download of the whole file
                        isEncrypted = await CheckIfSnapshotIsEncrypted(fileInfo.FullPath);
                    }
                }
                else if (isSnapshot)
                {
                    throw new InvalidOperationException($"Cannot have a snapshot backup file ({GetFileName(fileInfo.FullPath)}) after other backup files!");
                }

                firstFile = false;

                while (_sortedList.ContainsKey(fileInfo.LastModified))
                {
                    fileInfo.LastModified = fileInfo.LastModified.AddMilliseconds(1);
                }

                var folderDetails = ParseFolderNameFrom(fileInfo.DirectoryPath);

                _sortedList.Add(fileInfo.LastModified,
                    new RestorePoint
                    {
                        DateTime = fileInfo.LastModified,
                        Location = fileInfo.DirectoryPath,
                        FileName = fileInfo.FullPath,
                        IsSnapshotRestore = snapshotRestore,
                        IsIncremental = BackupUtils.IsIncrementalBackupFile(extension),
                        IsEncrypted = isEncrypted,
                        DatabaseName = folderDetails.DatabaseName,
                        NodeTag = folderDetails.NodeTag
                    });
            }
        }

        public static bool IsBackupOrSnapshot(string filePath)
        {
            var extension = Path.GetExtension(filePath);
            return
                BackupUtils.IsBackupFile(filePath) ||
                Constants.Documents.PeriodicBackup.SnapshotExtension.Equals(extension, StringComparison.OrdinalIgnoreCase) ||
                Constants.Documents.PeriodicBackup.EncryptedSnapshotExtension.Equals(extension, StringComparison.OrdinalIgnoreCase);
        }

        private async Task<bool> CheckIfSnapshotIsEncrypted(string fullPath)
        {
            using (var zip = await GetZipArchive(fullPath))
            {
                foreach (var zipEntry in zip.Entries)
                {
                    if (string.Equals(zipEntry.FullName, RestoreSettings.SettingsFileName, StringComparison.OrdinalIgnoreCase))
                    {
                        using (var entryStream = zipEntry.Open())
                        {
                            var json = await _context.ReadForMemoryAsync(entryStream, "read database settings");
                            json.BlittableValidation();

                            RestoreSettings restoreSettings = JsonDeserializationServer.RestoreSettings(json);
                            return restoreSettings.DatabaseRecord.Encrypted;
                        }
                    }
                }
            }

            throw new InvalidOperationException("Can't find settings file in backup archive.");
        }

        protected internal static string GetDirectoryName(string path, char delimiter = '/')
        {
            var index = path.LastIndexOf(delimiter);
            if (index <= 0)
                return string.Empty;

            return path.Substring(0, index + 1);
        }

        public class DescendedDateComparer : IComparer<DateTime>
        {
            public int Compare(DateTime x, DateTime y)
            {
                return Comparer<DateTime>.Default.Compare(y, x);
            }
        }
    }
}


