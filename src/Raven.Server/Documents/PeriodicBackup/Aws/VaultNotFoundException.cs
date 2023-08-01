using System;

namespace Raven.Server.Documents.PeriodicBackup.Aws
{
    public sealed class VaultNotFoundException : Exception
    {
        public VaultNotFoundException()
        {
        }

        public VaultNotFoundException(string message) : base(message)
        {
        }

        public VaultNotFoundException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }
}