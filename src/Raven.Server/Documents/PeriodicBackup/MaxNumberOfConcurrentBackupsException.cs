using System;

namespace Raven.Server.Documents.PeriodicBackup
{
    public class MaxNumberOfConcurrentBackupsException : Exception
    {
        public MaxNumberOfConcurrentBackupsException()
        {

        }

        public MaxNumberOfConcurrentBackupsException(string message) : base(message)
        {

        }

        public MaxNumberOfConcurrentBackupsException(string message, Exception inner) : base(message, inner)
        {

        }
    }
}
