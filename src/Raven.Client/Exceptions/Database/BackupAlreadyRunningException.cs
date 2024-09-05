using System;

namespace Raven.Client.Exceptions.Database
{
    public sealed class BackupAlreadyRunningException : RavenException
    {
        public long OperationId;
        public string NodeTag;

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
