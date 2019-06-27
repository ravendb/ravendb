using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Smuggler;
using Raven.Server.Documents.PeriodicBackup.Restore;

namespace Raven.Server.Documents.PeriodicBackup.Retention
{
    public abstract class RetentionPolicyRunnerBase
    {
        private readonly RetentionPolicy _retentionPolicy;
        private readonly string _databaseName;

        protected RetentionPolicyRunnerBase(RetentionPolicy retentionPolicy, string databaseName)
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
                (_retentionPolicy.MinimumBackupsToKeep == null && _retentionPolicy.MinimumBackupAgeToKeep == null))
                return; // no retention policy

            try
            {
                var folders = await GetFolders();

                var sortedBackupFolders = new SortedList<DateTime, string>();

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
                        // TODO: log this
                        // couldn't parse the backup time
                        continue;
                    }

                    if (string.Equals(folderDetails.DatabaseName, _databaseName, StringComparison.OrdinalIgnoreCase) == false)
                    {
                        // a backup for a different database
                        continue; // a backup for a different database
                    }

                    var files = await GetFiles(folder);
                    var hasFullBackupOrSnapshot = files.Any(BackupUtils.IsFullBackupOrSnapshot);
                    if (hasFullBackupOrSnapshot == false)
                    {
                        // no backup files
                        continue;
                    }

                    while (sortedBackupFolders.ContainsKey(backupTime))
                    {
                        backupTime = backupTime.AddMilliseconds(1);
                    }

                    sortedBackupFolders.Add(backupTime, folder);
                }

                // we are going to keep at least one backup
                var minimumBackupsToKeep = _retentionPolicy.MinimumBackupsToKeep ?? 1;

                if (sortedBackupFolders.Count <= minimumBackupsToKeep)
                {
                    // the number of backups to keep is more than we currently have
                    return;
                }

                var now = DateTime.UtcNow;
                var deleted = 0L;
                if (_retentionPolicy.MinimumBackupAgeToKeep.HasValue)
                {
                    foreach (var backupFolder in sortedBackupFolders)
                    {
                        if (now - backupFolder.Key < _retentionPolicy.MinimumBackupAgeToKeep)
                        {
                            // all backups are sorted by date
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
                    foreach (var backupFolder in sortedBackupFolders)
                    {
                        await DeleteFolder(backupFolder.Value);
                        deleted++;

                        if (ReachedMinimumBackupsToKeep())
                            break;
                    }
                }

                bool ReachedMinimumBackupsToKeep()
                {
                    return sortedBackupFolders.Count - deleted <= minimumBackupsToKeep;
                }
            }
            catch (NotSupportedException)
            {
                // TODO: log this
            }
            catch (Exception e)
            {
                // failure to delete backups shouldn't result in backup failure
                // TODO: log this
            }
        }
    }
}
