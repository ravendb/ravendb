using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
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
        private readonly Action<string> _onProgress;
        private readonly bool _isFullBackup;

        protected RetentionPolicyRunnerBase(RetentionPolicyBaseParameters parameters)
        {
            _retentionPolicy = parameters.RetentionPolicy;
            _databaseName = parameters.DatabaseName;
            _onProgress = parameters.OnProgress;
            _isFullBackup = parameters.IsFullBackup;
        }

        protected abstract Task<List<string>> GetFolders();

        protected abstract string GetFolderName(string folderPath);

        protected abstract Task<List<string>> GetFiles(string folder);

        protected abstract Task DeleteFolders(List<FolderDetails> folderDetails);

        protected abstract string Name { get; }

        public async Task Execute()
        {
            if (_isFullBackup == false)
                return; // run this only for full backups

            if (_retentionPolicy == null ||
                _retentionPolicy.Disabled ||
                _retentionPolicy.MinimumBackupAgeToKeep == null)
                return; // no retention policy

            try
            {
                var folders = await GetFolders();
                var sortedBackupFolders = new SortedList<DateTime, FolderDetails>();

                var sp = Stopwatch.StartNew();
                _onProgress.Invoke($"Got {folders.Count:#,#} potential backups");

                foreach (var folder in folders)
                {
                    var folderName = GetFolderName(folder);
                    var folderDetails = RestoreUtils.ParseFolderName(folderName);
                    if (folderDetails.BackupTimeAsString == null)
                    {
                        if (Logger.IsInfoEnabled)
                            Logger.Info($"Failed to get backup date time for folder: {folder}");
                        continue;
                    }

                    if (DateTime.TryParseExact(
                            folderDetails.BackupTimeAsString,
                            BackupTask.GetDateTimeFormat(folderDetails.BackupTimeAsString),
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
                        continue;
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

                var foldersToDelete = GetFoldersToDelete(sortedBackupFolders);
                await DeleteFolders(foldersToDelete);

                var message = $"Deleted {foldersToDelete.Count:#,#} backups, took: {sp.ElapsedMilliseconds:#,#}ms";
                _onProgress.Invoke(message);
                if (Logger.IsInfoEnabled)
                    Logger.Info(message);
            }
            catch (NotSupportedException)
            {
                if (Logger.IsInfoEnabled)
                    Logger.Info($"Retention Policy for {Name} isn't currently supported");
            }
            catch (OperationCanceledException)
            {
            }
            catch (Exception e)
            {
                // failure to delete backups shouldn't result in backup failure
                if (Logger.IsInfoEnabled)
                    Logger.Info($"Failed to run Retention Policy for {Name}", e);
            }
        }

        private List<FolderDetails> GetFoldersToDelete(SortedList<DateTime, FolderDetails> sortedBackupFolders)
        {
            // the time in the backup folder name is the local time
            var now = DateTime.Now;

            var foldersToDelete = new List<FolderDetails>();

            foreach (var backupFolder in sortedBackupFolders)
            {
                if (now - backupFolder.Key < _retentionPolicy.MinimumBackupAgeToKeep)
                {
                    // all backups are sorted by date
                    break;
                }

                foldersToDelete.Add(backupFolder.Value);
            }

            return foldersToDelete;
        }
    }
}
