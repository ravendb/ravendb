using System;

namespace Raven.Server.Documents.PeriodicBackup
{
    public class BackupDelayException : Exception
    {
        public TimeSpan DelayPeriod { get; set; } = TimeSpan.FromMinutes(1);

        public BackupDelayException()
        {

        }

        public BackupDelayException(string message) : base(message)
        {

        }

        public BackupDelayException(string message, Exception inner) : base(message, inner)
        {

        }
    }
}
