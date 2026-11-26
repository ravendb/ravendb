using System;

namespace Raven.Server.Documents.PeriodicBackup
{
    public sealed class BackupDelayException : Exception
    {
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
