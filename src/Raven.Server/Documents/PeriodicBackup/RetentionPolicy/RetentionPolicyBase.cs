using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Documents.Operations.Backups;
using Raven.Server.Documents.PeriodicBackup.Restore;

namespace Raven.Server.Documents.PeriodicBackup
{
    public abstract class RetentionPolicyBase
    {
        private readonly RetentionPolicy _retentionPolicy;
        private readonly string _databaseName;

        protected RetentionPolicyBase(RetentionPolicy retentionPolicy, string databaseName)
        {
            _retentionPolicy = retentionPolicy;
            _databaseName = databaseName;
        }

        public abstract Task<List<string>> GetFolders();

        public abstract Task<List<string>> GetFiles(string folder);

        public abstract Task DeleteFolder(string folder);

        public async Task Execute()
        {
            if (_retentionPolicy == null ||
                _retentionPolicy.Disabled ||
                _retentionPolicy.MinimumBackupsToKeep == null && _retentionPolicy.MinimumBackupAgeToKeep == null)
                return; // no retention policy

            if (_retentionPolicy.MinimumBackupsToKeep <= 0)
                return;

            try
            {
                var folders = await GetFolders();

                var allBackupFolders = new SortedList<DateTime, string>();

                foreach (var folder in folders)
                {
                    var folderDetails = RestoreUtils.ParseFolderName(folder);
                    if (DateTime.TryParseExact(
                            folderDetails.BackupTimeAsString,
                            BackupTask.DateTimeFormat,
                            CultureInfo.InvariantCulture,
                            DateTimeStyles.None,
                            out var backupTime) == false)
                    {
                        // couldn't parse the backup time
                        continue;
                    }

                    if (string.Equals(folderDetails.DatabaseName, _databaseName, StringComparison.OrdinalIgnoreCase) == false)
                        continue; // a backup for a different database

                    var files = await GetFiles(folder);
                    var hasFullBackupOrSnapshot = files.Any(IsFullBackupOrSnapshot);
                    if (hasFullBackupOrSnapshot == false)
                        continue; // no backup files

                    allBackupFolders.Add(backupTime, folder);

                    bool IsFullBackupOrSnapshot(string filePath)
                    {
                        var extension = Path.GetExtension(filePath);

                        return Constants.Documents.PeriodicBackup.FullBackupExtension.Equals(extension, StringComparison.OrdinalIgnoreCase) ||
                               Constants.Documents.PeriodicBackup.EncryptedFullBackupExtension.Equals(extension, StringComparison.OrdinalIgnoreCase) ||
                               Constants.Documents.PeriodicBackup.SnapshotExtension.Equals(extension, StringComparison.OrdinalIgnoreCase) ||
                               Constants.Documents.PeriodicBackup.EncryptedSnapshotExtension.Equals(extension, StringComparison.OrdinalIgnoreCase);
                    }
                }


                var minimumBackupsToKeep = _retentionPolicy.MinimumBackupsToKeep ?? long.MaxValue;
                if (allBackupFolders.Count <= minimumBackupsToKeep)
                {
                    // the number of backups to keep is more than we currently have
                    return;
                }

                var now = DateTime.UtcNow;
                var deleted = 0L;
                if (_retentionPolicy.MinimumBackupAgeToKeep.HasValue)
                {
                    foreach (var backupFolder in allBackupFolders)
                    {
                        if (now - backupFolder.Key < _retentionPolicy.MinimumBackupAgeToKeep)
                        {
                            // allBackupFolders is sorted by date
                            break;
                        }

                        await DeleteFolder(backupFolder.Value);
                        deleted++;

                        if (ReachedMinimumBackupsToKeep())
                            break;
                    }
                }
                else
                {
                    foreach (var backupFolder in allBackupFolders)
                    {
                        await DeleteFolder(backupFolder.Value);
                        deleted++;

                        if (ReachedMinimumBackupsToKeep())
                            break;
                    }
                }

                bool ReachedMinimumBackupsToKeep()
                {
                    return allBackupFolders.Count - deleted == minimumBackupsToKeep;
                }
            }
            catch (Exception e)
            {
                // failure to delete backups shouldn't result in backup failure
                // TODO: log this
            }
        }
    }
}
