using System;
using System.Collections.Generic;
using Raven.Client.Documents.Operations.Backups;
using Raven.Server.Commercial;
using Sparrow.Server.Logging;
using BackupConfiguration = Raven.Server.Config.Categories.BackupConfiguration;

namespace Raven.Server.Documents.PeriodicBackup
{
    public sealed class ConcurrentBackupsCounter
    {
        private readonly object _locker = new object();

        private readonly LicenseManager _licenseManager;
        private readonly Dictionary<string, int> _runningBackupsPerDatabase = new();
        private readonly bool _skipModifications;

        public int MaxNumberOfConcurrentBackups { get; private set; }

        internal event Action<bool> RunningBackupsChanged;

        public int CurrentNumberOfRunningBackups
        {
            get
            {
                lock (_locker)
                {
                    return _runningBackupsPerDatabase.Count;
                }
            }
        }

        public ConcurrentBackupsCounter(BackupConfiguration backupConfiguration, LicenseManager licenseManager)
        {
            _licenseManager = licenseManager;

            int numberOfCoresToUse;
            var skipModifications = backupConfiguration.MaxNumberOfConcurrentBackups != null;
            if (skipModifications)
            {
                numberOfCoresToUse = backupConfiguration.MaxNumberOfConcurrentBackups.Value;
            }
            else
            {
                var utilizedCores = _licenseManager.GetCoresLimitForNode(out _, false);
                numberOfCoresToUse = GetNumberOfCoresToUseForBackup(utilizedCores);
            }

            MaxNumberOfConcurrentBackups = numberOfCoresToUse;
            _skipModifications = skipModifications;
        }

        public void TakePlace(string databaseName, string backupName, RavenLogger logger, bool enforceLimit)
        {
            if (TryTakePlace(databaseName, backupName, logger, enforceLimit, canTakeNewPlace: true))
                return;

            throw new BackupDelayException(
                $"Failed to start Backup Task: '{backupName}'. " +
                $"The task exceeds the maximum number of concurrent backup tasks configured. " +
                $"Current maximum number of concurrent backups is: {MaxNumberOfConcurrentBackups:#,#;;0}");
        }

        internal bool TryTakePlace(string databaseName, string backupName, RavenLogger logger, bool enforceLimit, bool canTakeNewPlace)
        {
            lock (_locker)
            {
                // Counts databases that are backing up, not backups: all shards and tasks of one database share one place.
                if (_runningBackupsPerDatabase.TryGetValue(databaseName, out var runningBackups))
                {
                    _runningBackupsPerDatabase[databaseName] = runningBackups + 1;
                    return true;
                }

                if (canTakeNewPlace == false)
                    return false;

                if (enforceLimit && MaxNumberOfConcurrentBackups - _runningBackupsPerDatabase.Count <= 0)
                    return false;

                _runningBackupsPerDatabase[databaseName] = 1;
            }

            RunningBackupsChanged?.Invoke(false);

            if (logger.IsInfoEnabled)
                logger.Info($"Starting backup task '{backupName}' of database '{databaseName}'");

            return true;
        }

        internal void GiveBackPlace(string databaseName)
        {
            RunningBackupsChanged?.Invoke(RemoveRunningBackup(databaseName));
        }

        private bool RemoveRunningBackup(string databaseName)
        {
            lock (_locker)
            {
                if (_runningBackupsPerDatabase.TryGetValue(databaseName, out var runningBackups) == false)
                    throw new InvalidOperationException("Tried to finish a backup which wasn't even started!");

                var wasFull = _runningBackupsPerDatabase.Count >= MaxNumberOfConcurrentBackups;

                if (runningBackups - 1 == 0)
                {
                    _runningBackupsPerDatabase.Remove(databaseName);
                }
                else
                {
                    _runningBackupsPerDatabase[databaseName] = runningBackups - 1;
                }

                return wasFull && _runningBackupsPerDatabase.Count < MaxNumberOfConcurrentBackups;
            }
        }

        public void FinishBackup(string databaseName, string backupName, PeriodicBackupStatus backupStatus, TimeSpan? elapsed, RavenLogger logger)
        {
            var fullLimitFreed = RemoveRunningBackup(databaseName);

            RunningBackupsChanged?.Invoke(fullLimitFreed);

            if (logger.IsInfoEnabled)
            {
                string backupTypeString = "backup";
                string extendedBackupTimings = string.Empty;
                if (backupStatus != null)
                {
                    backupTypeString = BackupTask.GetBackupDescription(backupStatus.BackupType, backupStatus.IsFull);

                    var first = true;
                    AddBackupTimings(backupStatus.LocalBackup, "local");
                    AddBackupTimings(backupStatus.UploadToS3, "Amazon S3");
                    AddBackupTimings(backupStatus.UploadToGlacier, "Amazon Glacier");
                    AddBackupTimings(backupStatus.UploadToAzure, "Azure");
                    AddBackupTimings(backupStatus.UploadToGoogleCloud, "Google Cloud");
                    AddBackupTimings(backupStatus.UploadToFtp, "FTP");

                    void AddBackupTimings(BackupStatus perDestinationBackupStatus, string backupTypeName)
                    {
                        if (perDestinationBackupStatus == null ||
                            perDestinationBackupStatus is CloudUploadStatus cus && cus.Skipped)
                            return;

                        if (first == false)
                            extendedBackupTimings += ", ";

                        first = false;
                        extendedBackupTimings +=
                            $"backup to {backupTypeName} took: " +
                            $"{(backupStatus.IsFull ? perDestinationBackupStatus.FullBackupDurationInMs : perDestinationBackupStatus.IncrementalBackupDurationInMs)}ms";
                    }
                }

                var message = $"Finished {backupTypeString} task '{backupName}'";
                if (elapsed != null)
                    message += $", took: {elapsed}";

                message += $" {extendedBackupTimings}";

                logger.Info(message);
            }
        }

        public void ModifyMaxConcurrentBackups()
        {
            if (_skipModifications)
                return;

            var utilizedCores = _licenseManager.GetCoresLimitForNode(out _);
            var newMaxConcurrentBackups = GetNumberOfCoresToUseForBackup(utilizedCores);

            if (MaxNumberOfConcurrentBackups == newMaxConcurrentBackups)
                return;

            lock (_locker)
            {
                MaxNumberOfConcurrentBackups = newMaxConcurrentBackups;
            }
        }

        public int GetNumberOfCoresToUseForBackup(int utilizedCores)
        {
            return Math.Max(1, utilizedCores / 2);
        }
    }
}
