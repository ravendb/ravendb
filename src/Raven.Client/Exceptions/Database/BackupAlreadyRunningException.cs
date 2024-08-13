using System;

namespace Raven.Client.Exceptions.Database
{
    public sealed class BackupAlreadyRunningException : RavenException
    {
        public BackupAlreadyRunningException(string message)
            : base(message)
        {
        }

        public BackupAlreadyRunningException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }
}
