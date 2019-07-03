using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Smuggler;
using Raven.Server.Documents.PeriodicBackup.Restore;
using Sparrow.Logging;

namespace Raven.Server.Documents.PeriodicBackup.Retention
{
    public abstract class RetentionPolicyRunnerBase
    {
        private static readonly Logger Logger = LoggingSource.Instance.GetLogger<RetentionPolicyRunnerBase>("BackupTask");

        private readonly RetentionPolicy _retentionPolicy;
        private readonly string _databaseName;

        protected RetentionPolicyRunnerBase(RetentionPolicy retentionPolicy, string databaseName)
        {
            _retentionPolicy = retentionPolicy;
            _databaseName = databaseName;
        }

        protected abstract Task<List<string>> GetFolders();

        protected abstract string GetFolderName(string folderPath);

        protected abstract Task<List<string>> GetFiles(string folder);

        protected abstract Task DeleteFolders(List<FolderDetails> folderDetails);

        public abstract string Name { get; }

        public async Task Execute()
        {
            if (_retentionPolicy == null ||
                _retentionPolicy.Disabled ||
                (_retentionPolicy.MinimumBackupsToKeep == null && _retentionPolicy.MinimumBackupAgeToKeep == null))
                return; // no retention policy

            try
            {
                var folders = await GetFolders();

                var sortedBackupFolders = new SortedList<DateTime, FolderDetails>();

                foreach (var folder in folders.OrderBy(x => x))
                {
                    var folderName = GetFolderName(folder);
                    var folderDetails = RestoreUtils.ParseFolderName(folderName);
                    if (DateTime.TryParseExact(
                            folderDetails.BackupTimeAsString,
                            folderDetails.BackupTimeAsString.Length == 16 ? BackupTask.LegacyDateTimeFormat : BackupTask.DateTimeFormat,
                            CultureInfo.InvariantCulture,
                            DateTimeStyles.None,
                            out var backupTime) == false)
                    {
                        if (Logger.IsInfoEnabled)
                            Logger.Info($"Failed to parse backup date time for folder: {folder}");
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

                    sortedBackupFolders.Add(backupTime, new FolderDetails
                    {
                        Name = folder,
                        Files = files
                    });
                }

                // we are going to keep at least one backup
                var minimumBackupsToKeep = _retentionPolicy.MinimumBackupsToKeep ?? 1;

                if (sortedBackupFolders.Count <= minimumBackupsToKeep)
                {
                    // the number of backups to keep is more than we currently have
                    return;
                }

                // the time in the backup folder name is the local time
                var now = DateTime.Now;

                var deleted = 0L;
                var foldersToDelete = new List<FolderDetails>();
                if (_retentionPolicy.MinimumBackupAgeToKeep.HasValue)
                {
                    foreach (var backupFolder in sortedBackupFolders)
                    {
                        if (now - backupFolder.Key < _retentionPolicy.MinimumBackupAgeToKeep)
                        {
                            // all backups are sorted by date
                            break;
                        }

                        foldersToDelete.Add(backupFolder.Value);
                        deleted++;

                        if (ReachedMinimumBackupsToKeep())
                            break;
                    }
                }
                else
                {
                    foreach (var backupFolder in sortedBackupFolders)
                    {
                        foldersToDelete.Add(backupFolder.Value);
                        deleted++;

                        if (ReachedMinimumBackupsToKeep())
                            break;
                    }
                }

                await DeleteFolders(foldersToDelete);

                foreach (var toDelete in foldersToDelete)
                {
                    Console.WriteLine($"Deleted: {Path.GetFileName(toDelete.Name)}");
                }

                foreach (var folder in (await GetFolders()))
                {
                    Console.WriteLine($"Existing: {Path.GetFileName(folder)}");
                }

                bool ReachedMinimumBackupsToKeep()
                {
                    return sortedBackupFolders.Count - deleted <= minimumBackupsToKeep;
                }
            }
            catch (NotSupportedException)
            {
                if (Logger.IsInfoEnabled)
                    Logger.Info($"Retention Policy for {Name} isn't currently supported");
            }
            catch (Exception e)
            {
                // failure to delete backups shouldn't result in backup failure
                if (Logger.IsInfoEnabled)
                    Logger.Info($"Failed to run Retention Policy for {Name}", e);
            }
        }
    }
}
