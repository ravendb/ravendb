using System;
using Raven.Client.Util;
using Raven.Server.Commercial;

namespace Raven.Server.Documents.PeriodicBackup
{
    public class ConcurrentBackupsCounter
    {
        private int _numberOfCoresToUse;
        private int _maxConcurrentBackups;
        private readonly bool _skipModifications;

        public ConcurrentBackupsCounter(int? maxNumberOfConcurrentBackupsConfiguration, LicenseManager licenseManager)
        {
            int numberOfCoresToUse;
            var skipModifications = false;
            if (maxNumberOfConcurrentBackupsConfiguration != null)
            {
                numberOfCoresToUse = maxNumberOfConcurrentBackupsConfiguration.Value;
                skipModifications = true;
            }
            else
            {
                licenseManager.GetCoresLimitForNode(out var utilizedCores);
                numberOfCoresToUse = GetNumberOfCoresToUse(utilizedCores);
            }

            _numberOfCoresToUse = numberOfCoresToUse;
            _maxConcurrentBackups = numberOfCoresToUse;
            _skipModifications = skipModifications;
        }

        public IDisposable StartBackup(string backupName)
        {
            lock (this)
            {
                if (_numberOfCoresToUse <= 0)
                {
                    throw new BackupDelayException(
                        $"Failed to start Backup Task: '{backupName}'. " +
                        $"The task exceeds the maximum number of concurrent backup tasks configured. " +
                        $"Current maximum number of concurrent backups is: {_maxConcurrentBackups:#,#;;0}")
                    {
                        DelayPeriod = TimeSpan.FromMinutes(1)
                    };
                }

                _numberOfCoresToUse--;
            }

            return new DisposableAction(() =>
            {
                lock (this)
                {
                    _numberOfCoresToUse++;
                }
            });
        }

        public void SetMaxNumberOfConcurrentBackups(int utilizedCores)
        {
            if (_skipModifications)
                return;

            lock (this)
            {
                var newMaxConcurrentBackups = GetNumberOfCoresToUse(utilizedCores);
                var diff = newMaxConcurrentBackups - _maxConcurrentBackups;
                _maxConcurrentBackups = newMaxConcurrentBackups;
                _numberOfCoresToUse += diff;
            }
        }

        public int GetNumberOfCoresToUse(int utilizedCores)
        {
            return Math.Max(1, utilizedCores / 2);
        }
    }
}
