using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using Raven.Client;

namespace Raven.Server.Documents.PeriodicBackup
{
    public class Restore
    {
        public static void FetchRestorePoints(string directoryPath, List<RestorePoint> restorePoints)
        {
            const string legacyIncrementalBackupExtension = "ravendb-incremental-dump";
            const string legacyFullBackupExtension = "ravendb-full-dump";

            var files = Directory.GetFiles(directoryPath)
                .Where(filePath =>
                {
                    var extension = Path.GetExtension(filePath);
                    return
                        Constants.Documents.PeriodicBackup.IncrementalBackupExtension.Equals(extension, StringComparison.OrdinalIgnoreCase) ||
                        Constants.Documents.PeriodicBackup.FullBackupExtension.Equals(extension, StringComparison.OrdinalIgnoreCase) ||
                        Constants.Documents.PeriodicBackup.SnapshotExtension.Equals(extension, StringComparison.OrdinalIgnoreCase) ||
                        legacyIncrementalBackupExtension.Equals(extension, StringComparison.OrdinalIgnoreCase) ||
                        legacyFullBackupExtension.Equals(extension, StringComparison.OrdinalIgnoreCase);
                })
                .OrderBy(x => x);

            var filesCount = 0;
            var firstFile = true;
            var snapshotRestore = false;
            foreach (var filePath in files)
            {
                var extension = Path.GetExtension(filePath);
                var isSnapshot = Constants.Documents.PeriodicBackup.SnapshotExtension.Equals(extension, StringComparison.OrdinalIgnoreCase);
                if (firstFile)
                {
                    snapshotRestore = isSnapshot;
                }
                else if (isSnapshot)
                {
                    throw new InvalidOperationException($"Cannot have a snapshot backup file ({Path.GetFileName(filePath)}) after other backup files!");
                }

                firstFile = false;
                filesCount++;

                var fileNameWithoutExtension = Path.GetFileNameWithoutExtension(filePath);
                var fileDate = TryExtractDateFromFileName(fileNameWithoutExtension, filePath);
                restorePoints.Insert(0, new RestorePoint
                {
                    Key = fileDate,
                    Details = new RestorePointDetails
                    {
                        Location = directoryPath,
                        FileName = Path.GetFileName(filePath),
                        IsSnapshotRestore = snapshotRestore,
                        FilesToRestore = filesCount
                    }
                });
            }
        }

        private static string TryExtractDateFromFileName(string fileName, string filePath)
        {
            DateTime result;
            if (DateTime.TryParseExact(
                    fileName,
                    PeriodicBackupRunner.DateTimeFormat,
                    CultureInfo.InvariantCulture,
                    DateTimeStyles.None,
                    out result) == false)
            {
                result = File.GetLastWriteTime(filePath).ToUniversalTime();
            }

            return result.ToString("yyyy/MM/dd HH:mm");
        }
    }
}