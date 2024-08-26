using System;
using System.Collections.Generic;
using Raven.Client.Documents.Operations.Backups;
using Raven.Server.Commercial;
using Sparrow.Logging;
using BackupConfiguration = Raven.Server.Config.Categories.BackupConfiguration;

namespace Raven.Server.Documents.PeriodicBackup
{
    public sealed class ConcurrentBackupsCounter
    {
        private readonly object _locker = new object();

        private readonly LicenseManager _licenseManager;
        private readonly Dictionary<string, int> _runningBackupsPerDatabase = new();
        private int _maxConcurrentBackups;
        private readonly TimeSpan _concurrentBackupsDelay;
        private readonly bool _skipModifications;

        public int MaxNumberOfConcurrentBackups
        {
            get
            {
                lock (_locker)
                {
                    return _maxConcurrentBackups;
                }
            }
        }

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

            _maxConcurrentBackups = numberOfCoresToUse;
            _concurrentBackupsDelay = backupConfiguration.ConcurrentBackupsDelay.AsTimeSpan;
            _skipModifications = skipModifications;
        }

        public void StartBackup(string databaseName, string backupName, RavenLogger logger)
        {
            lock (_locker)
            {
                if (_runningBackupsPerDatabase.TryGetValue(databaseName, out var runningBackups))
                {
                    _runningBackupsPerDatabase[databaseName] = runningBackups + 1;
                    return;
                }

                if (_maxConcurrentBackups - _runningBackupsPerDatabase.Count <= 0)
                {
                    throw new BackupDelayException(
                        $"Failed to start Backup Task: '{backupName}'. " +
                        $"The task exceeds the maximum number of concurrent backup tasks configured. " +
                        $"Current maximum number of concurrent backups is: {_maxConcurrentBackups:#,#;;0}")
                    {
                        DelayPeriod = _concurrentBackupsDelay
                    };
                }

                _runningBackupsPerDatabase[databaseName] = 1;
            }

            if (logger.IsInfoEnabled)
                logger.Info($"Starting backup task '{backupName}'");
        }

        public void FinishBackup(string databaseName, string backupName, PeriodicBackupStatus backupStatus, TimeSpan? elapsed, RavenLogger logger)
        {
            lock (_locker)
            {
                if (_runningBackupsPerDatabase.TryGetValue(databaseName, out var runningBackups) == false)
                    throw new InvalidOperationException("Tried to finish a backup which wasn't even started!");

                if (runningBackups - 1 == 0)
                {
                    _runningBackupsPerDatabase.Remove(databaseName);
                }
                else
                {
                    _runningBackupsPerDatabase[databaseName] = runningBackups - 1;
                }
            }

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

            lock (_locker)
            {
                _maxConcurrentBackups = newMaxConcurrentBackups;
            }
        }

        public int GetNumberOfCoresToUseForBackup(int utilizedCores)
        {
            return Math.Max(1, utilizedCores / 2);
        }
    }
}
