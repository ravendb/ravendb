using System;
using System.Collections.Generic;
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
        protected static readonly Regex BackupFolderRegex = new Regex(@".ravendb-(.+)-([A-Za-z]+)-(.+)$", RegexOptions.Compiled);
        protected static readonly Regex FileNameRegex = new Regex(@"([0-9]{4}-[0-9]{2}-[0-9]{2}-[0-9]{2}-[0-9]{2})", RegexOptions.Compiled);

        private readonly SortedList<DateTime, RestorePoint> _sortedList;
        private readonly TransactionOperationContext _context;

        protected RestorePointsBase(SortedList<DateTime, RestorePoint> sortedList, TransactionOperationContext context)
        {
            _sortedList = sortedList;
            _context = context;
        }

        public abstract Task FetchRestorePoints(string path);

        protected abstract Task<List<FileInfoDetails>> GetFiles(string path);

        protected abstract (string DatabaseName, string NodeTag) ParseFolderName(string path);

        protected abstract Task<ZipArchive> GetZipArchive(string filePath);

        protected abstract string GetFileName(string fullPath);

        public abstract void Dispose();

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
                .ThenBy(x => x.LastModified);

            var folderDetails = ParseFolderName(path);
            var filesCount = 0;
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
                    else
                    {
                        if (isSnapshot)
                            isEncrypted = await CheckIfSnapshotIsEncrypted(fileInfo.FullPath);
                    }
                }
                else if (isSnapshot)
                {
                    throw new InvalidOperationException($"Cannot have a snapshot backup file ({GetFileName(fileInfo.FullPath)}) after other backup files!");
                }

                firstFile = false;
                filesCount++;

                while (_sortedList.ContainsKey(fileInfo.LastModified))
                {
                    fileInfo.LastModified = fileInfo.LastModified.AddMilliseconds(1);
                }

                _sortedList.Add(fileInfo.LastModified,
                    new RestorePoint
                    {
                        DateTime = fileInfo.LastModified,
                        Location = path,
                        FileName = GetFileName(fileInfo.FullPath),
                        IsSnapshotRestore = snapshotRestore,
                        IsIncremental = BackupUtils.IsIncrementalBackupFile(extension),
                        IsEncrypted = isEncrypted,
                        FilesToRestore = filesCount,
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
                            var json = _context.Read(entryStream, "read database settings");
                            json.BlittableValidation();

                            RestoreSettings restoreSettings = JsonDeserializationServer.RestoreSettings(json);
                            return restoreSettings.DatabaseRecord.Encrypted;
                        }
                    }
                }
            }

            throw new InvalidOperationException("Can't find settings file in backup archive.");
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

    
