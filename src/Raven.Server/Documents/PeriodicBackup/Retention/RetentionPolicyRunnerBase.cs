using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Threading;
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
        private readonly bool _isFullBackup;
        private readonly Action<string> _onProgress;
        protected CancellationToken CancellationToken;

        protected RetentionPolicyRunnerBase(RetentionPolicyBaseParameters parameters)
        {
            _retentionPolicy = parameters.RetentionPolicy;
            _databaseName = parameters.DatabaseName;
            _isFullBackup = parameters.IsFullBackup;
            _onProgress = parameters.OnProgress;
            CancellationToken = parameters.CancellationToken;
        }

        protected abstract Task<GetFoldersResult> GetSortedFolders();

        protected abstract string GetFolderName(string folderPath);

        protected abstract Task<string> GetFirstFileInFolder(string folder);

        protected abstract Task DeleteFolders(List<string> folders);

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
                var sp = Stopwatch.StartNew();
                var foldersToDelete = new List<string>();
                var now = DateTime.Now; // the time in the backup folder name is the local time
                var deletingAllFolders = true;

                do
                {
                    var folders = await GetSortedFolders();
                    _onProgress.Invoke($"Got {folders.List.Count:#,#} potential backups, has more: {folders.HasMore}");

                    var canContinue = await UpdateFoldersToDelete(folders, now, foldersToDelete);
                    if (canContinue == false)
                    {
                        deletingAllFolders = false;
                        break;
                    }

                    if (folders.HasMore == false)
                        break;

                } while (true);

                if (foldersToDelete.Count == 0)
                    return;

                if (deletingAllFolders)
                {
                    throw new InvalidOperationException("Trying to delete all backup folders, did you modify the backup folders while the backup was running?");
                }

                var message = $"Found {foldersToDelete.Count:#,#} backups to delete, took: {sp.ElapsedMilliseconds:#,#}ms";
                _onProgress.Invoke(message);
                if (Logger.IsInfoEnabled)
                    Logger.Info(message);

                sp.Restart();
                await DeleteFolders(foldersToDelete);

                message = $"Deleted {foldersToDelete.Count:#,#} backups, took: {sp.ElapsedMilliseconds:#,#}ms";
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

        private async Task<bool> UpdateFoldersToDelete(GetFoldersResult folders, DateTime now, List<string> foldersToDelete)
        {
            foreach (var folder in folders.List)
            {
                CancellationToken.ThrowIfCancellationRequested();

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

                if (now - backupTime < _retentionPolicy.MinimumBackupAgeToKeep)
                {
                    // all backups are sorted by date
                    return false;
                }

                if (string.Equals(folderDetails.DatabaseName, _databaseName, StringComparison.OrdinalIgnoreCase) == false)
                    continue; // a backup for a different database

                var firstFile = await GetFirstFileInFolder(folder);
                if (firstFile == null)
                    continue; // folder is empty

                var hasFullBackupOrSnapshot = BackupUtils.IsFullBackupOrSnapshot(firstFile);
                if (hasFullBackupOrSnapshot == false)
                    continue; // no snapshot or full backup

                foldersToDelete.Add(folder);
            }

            return true;
        }
    }
}
