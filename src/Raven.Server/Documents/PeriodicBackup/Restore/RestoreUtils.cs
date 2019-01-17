using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text.RegularExpressions;
using Raven.Client;
using Raven.Client.Documents.Smuggler;
using Raven.Server.Json;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.PeriodicBackup.Restore
{
    public static class RestoreUtils
    {
        private static readonly Regex BackupFolderRegex = new Regex(@".ravendb-(.+)-([A-Za-z]+)-(.+)$", RegexOptions.Compiled);
        private static readonly Regex FileNameRegex = new Regex(@"([0-9]{4}-[0-9]{2}-[0-9]{2}-[0-9]{2}-[0-9]{2})", RegexOptions.Compiled);

        public static void FetchRestorePoints(
            string directoryPath,
            SortedList<DateTime, RestorePoint> sortedList,
            TransactionOperationContext context,
            bool assertLegacyBackups = false)
        {
            const string legacyEsentBackupFile = "RavenDB.Backup";
            const string legacyVoronBackupFile = "RavenDB.Voron.Backup";

            var files = Directory.GetFiles(directoryPath)
                .Where(filePath =>
                {
                    if (assertLegacyBackups)
                    {
                        var fileName = Path.GetFileName(filePath);
                        if (fileName.Equals(legacyEsentBackupFile, StringComparison.OrdinalIgnoreCase) ||
                            fileName.Equals(legacyVoronBackupFile, StringComparison.OrdinalIgnoreCase))
                        {
                            throw new InvalidOperationException("Cannot restore a legacy backup (v3.x and below). " +
                                                                "You can restore a v3.x periodic export backup or " +
                                                                "use an export from v3.x and import it using the studio");
                        }
                    }

                    return IsBackupOrSnapshot(filePath);
                })
                .OrderBackups();

            var folderDetails = ParseFolderName(directoryPath);
            var filesCount = 0;
            var firstFile = true;
            var snapshotRestore = false;
            var isEncrypted = false;
            
            foreach (var filePath in files)
            {
                var extension = Path.GetExtension(filePath);
                var isSnapshot = ((Constants.Documents.PeriodicBackup.SnapshotExtension.Equals(extension, StringComparison.OrdinalIgnoreCase)) ||
                                  (Constants.Documents.PeriodicBackup.EncryptedSnapshotExtension.Equals(extension, StringComparison.OrdinalIgnoreCase)));
                if (firstFile)
                {
                    snapshotRestore = isSnapshot;
                    if ((Constants.Documents.PeriodicBackup.EncryptedSnapshotExtension.Equals(extension, StringComparison.OrdinalIgnoreCase)) ||
                         (Constants.Documents.PeriodicBackup.EncryptedFullBackupExtension.Equals(extension, StringComparison.OrdinalIgnoreCase)))
                    {
                        isEncrypted = true;
                    }
                    else
                        isEncrypted = (isSnapshot && CheckIfSnapshotIsEncrypted(filePath, context));
                }
                else if (isSnapshot)
                {
                    throw new InvalidOperationException($"Cannot have a snapshot backup file ({Path.GetFileName(filePath)}) after other backup files!");
                }

                firstFile = false;
                filesCount++;

                var fileNameWithoutExtension = Path.GetFileNameWithoutExtension(filePath);
                var fileDate = TryExtractDateFromFileName(fileNameWithoutExtension, filePath);
                while (sortedList.ContainsKey(fileDate))
                {
                    fileDate = fileDate.AddMilliseconds(1);
                }

                sortedList.Add(fileDate, new RestorePoint
                {
                    DateTime = fileDate,
                    Location = directoryPath,
                    FileName = Path.GetFileName(filePath),
                    IsSnapshotRestore = snapshotRestore,
                    IsIncremental = BackupUtils.IsIncrementalBackupFile(extension),
                    IsEncrypted = isEncrypted,
                    FilesToRestore = filesCount,
                    DatabaseName = folderDetails.DatabaseName,
                    NodeTag = folderDetails.NodeTag
                });
            }
        }

        private static bool CheckIfSnapshotIsEncrypted(string filePath, TransactionOperationContext context)
        {
            using (var zip = ZipFile.Open(filePath, ZipArchiveMode.Read, System.Text.Encoding.UTF8))
            {
                foreach (var zipEntry in zip.Entries)
                {
                    if (string.Equals(zipEntry.FullName, RestoreSettings.SettingsFileName, StringComparison.OrdinalIgnoreCase))
                    {
                        using (var entryStream = zipEntry.Open())
                        {
                            var json = context.Read(entryStream, "read database settings");
                            json.BlittableValidation();

                            RestoreSettings restoreSettings = JsonDeserializationServer.RestoreSettings(json);
                            return restoreSettings.DatabaseRecord.Encrypted;
                        }
                    }
                }
            }

            throw new InvalidOperationException("Can't find settings file in backup archive.");
        }

        public static bool IsBackupOrSnapshot(string filePath)
        {
            var extension = Path.GetExtension(filePath);
            return
                BackupUtils.IsBackupFile(filePath) ||
                Constants.Documents.PeriodicBackup.SnapshotExtension.Equals(extension, StringComparison.OrdinalIgnoreCase) ||
                Constants.Documents.PeriodicBackup.EncryptedSnapshotExtension.Equals(extension, StringComparison.OrdinalIgnoreCase);
        }

        private static DateTime TryExtractDateFromFileName(string fileName, string filePath)
        {
            // file name format: 2017-06-01-00-00-00
            // legacy incremental backup format: 2017-06-01-00-00-00-0

            var match = FileNameRegex.Match(fileName);
            if (match.Success)
            {
                fileName = match.Value;
            }
            if (DateTime.TryParseExact(
                    fileName,
                    BackupTask.DateTimeFormat,
                    CultureInfo.InvariantCulture,
                    DateTimeStyles.None,
                    out DateTime result) == false)
            {
                result = File.GetLastWriteTimeUtc(filePath).ToLocalTime();
            }

            return result;
        }

        private static (string DatabaseName, string NodeTag) ParseFolderName(string directoryPath)
        {
            // [Date].ravendb-[Database Name]-[Node Tag]-[Backup Type]
            // [DATE] - format: "yyyy-MM-dd-HH-mm"
            // [Backup Type] - backup/snapshot
            // example: //2018-02-03-15-34.ravendb-Northwind-A-backup

            var lastFolderName = Path.GetFileName(directoryPath);
            var match = BackupFolderRegex.Match(lastFolderName);
            return match.Success
                ? (match.Groups[1].Value, match.Groups[2].Value)
                : (null, null);
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
