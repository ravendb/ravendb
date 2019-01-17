using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace Raven.Client.Documents.Smuggler
{
    internal static class BackupUtils
    {
        private const string LegacyIncrementalBackupExtension = ".ravendb-incremental-dump";
        private const string LegacyFullBackupExtension = ".ravendb-full-dump";

        internal static bool IsBackupFile(string filePath)
        {
            var extension = Path.GetExtension(filePath);
            return
                Constants.Documents.PeriodicBackup.IncrementalBackupExtension.Equals(extension, StringComparison.OrdinalIgnoreCase) ||
                Constants.Documents.PeriodicBackup.FullBackupExtension.Equals(extension, StringComparison.OrdinalIgnoreCase) ||
                Constants.Documents.PeriodicBackup.EncryptedFullBackupExtension.Equals(extension, StringComparison.OrdinalIgnoreCase) ||
                LegacyIncrementalBackupExtension.Equals(extension, StringComparison.OrdinalIgnoreCase) ||
                LegacyFullBackupExtension.Equals(extension, StringComparison.OrdinalIgnoreCase);
        }

        internal static bool IsIncrementalBackupFile(string extension)
        {
            return
                Constants.Documents.PeriodicBackup.IncrementalBackupExtension.Equals(extension, StringComparison.OrdinalIgnoreCase) ||
                LegacyIncrementalBackupExtension.Equals(extension, StringComparison.OrdinalIgnoreCase);
        }

        internal static IOrderedEnumerable<string> OrderBackups(this IEnumerable<string> data)
        {
            return data
                .OrderBy(Path.GetFileNameWithoutExtension)
                .ThenBy(Path.GetExtension, PeriodicBackupFileExtensionComparer.Instance)
                .ThenBy(File.GetLastWriteTimeUtc);
        }
    }
}
