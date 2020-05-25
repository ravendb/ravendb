using System;
using Raven.Server.Commercial;

namespace Raven.Server.Documents.PeriodicBackup
{
    public class ConcurrentBackupsCounter
    {
        private readonly object _locker = new object();

        private readonly LicenseManager _licenseManager;
        private int _concurrentBackups;
        private int _maxConcurrentBackups;
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
                    return _maxConcurrentBackups - _concurrentBackups;
                }
            }
        }

        public ConcurrentBackupsCounter(int? maxNumberOfConcurrentBackupsConfiguration, LicenseManager licenseManager)
        {
            _licenseManager = licenseManager;

            int numberOfCoresToUse;
            var skipModifications = maxNumberOfConcurrentBackupsConfiguration != null;
            if (skipModifications)
            {
                numberOfCoresToUse = maxNumberOfConcurrentBackupsConfiguration.Value;
            }
            else
            {
                var utilizedCores = _licenseManager.GetCoresLimitForNode();
                numberOfCoresToUse = GetNumberOfCoresToUseForBackup(utilizedCores);
            }

            _concurrentBackups = numberOfCoresToUse;
            _maxConcurrentBackups = numberOfCoresToUse;
            _skipModifications = skipModifications;
        }

        public void StartBackup(string backupName)
        {
            lock (_locker)
            {
                if (_concurrentBackups <= 0)
                {
                    throw new BackupDelayException(
                        $"Failed to start Backup Task: '{backupName}'. " +
                        $"The task exceeds the maximum number of concurrent backup tasks configured. " +
                        $"Current maximum number of concurrent backups is: {_maxConcurrentBackups:#,#;;0}")
                    {
                        DelayPeriod = TimeSpan.FromMinutes(1)
                    };
                }

                _concurrentBackups--;
            }
        }

        public void FinishBackup()
        {
            lock (_locker)
            {
                _concurrentBackups++;
            }
        }

        public void ModifyMaxConcurrentBackups()
        {
            if (_skipModifications)
                return;

            var utilizedCores = _licenseManager.GetCoresLimitForNode();
            var newMaxConcurrentBackups = GetNumberOfCoresToUseForBackup(utilizedCores);

            lock (_locker)
            {
                var diff = newMaxConcurrentBackups - _maxConcurrentBackups;
                _maxConcurrentBackups = newMaxConcurrentBackups;
                _concurrentBackups += diff;
            }
        }

        public int GetNumberOfCoresToUseForBackup(int utilizedCores)
        {
            return Math.Max(1, utilizedCores / 2);
        }
    }
}
