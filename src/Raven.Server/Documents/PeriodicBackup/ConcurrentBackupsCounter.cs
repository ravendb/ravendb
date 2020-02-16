using System;
using Raven.Client.Util;

namespace Raven.Server.Documents.PeriodicBackup
{
    public class ConcurrentBackupsCounter
    {
        private int _numberOfCoresToUse;
        private int _maxConcurrentBackups;

        public ConcurrentBackupsCounter(int numberOfCoresToUse)
        {
            _numberOfCoresToUse = numberOfCoresToUse;
            _maxConcurrentBackups = numberOfCoresToUse;
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

        public void SetMaxNumberOfConcurrentBackups(int newMaxConcurrentBackups)
        {
            lock (this)
            {
                var diff = newMaxConcurrentBackups - _maxConcurrentBackups;
                _maxConcurrentBackups = newMaxConcurrentBackups;
                _numberOfCoresToUse += diff;
            }
        }
    }
}
