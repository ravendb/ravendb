using System;

namespace Raven.Server.Documents.PeriodicBackup.Azure
{
    public sealed class ContainerNotFoundException : Exception
    {
        public ContainerNotFoundException()
        {
        }

        public ContainerNotFoundException(string message) : base(message)
        {
        }
    }
}