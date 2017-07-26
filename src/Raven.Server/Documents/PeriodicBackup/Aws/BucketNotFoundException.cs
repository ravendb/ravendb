using System;

namespace Raven.Server.Documents.PeriodicBackup.Aws
{
    public class BucketNotFoundException : Exception
    {
        public BucketNotFoundException()
        {
        }

        public BucketNotFoundException(string message) : base(message)
        {
        }

        public BucketNotFoundException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }
}