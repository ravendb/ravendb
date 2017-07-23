using System;

namespace Raven.Server.Documents.PeriodicBackup.Azure
{
    public class ContainerNotFoundException : Exception
    {
        public ContainerNotFoundException()
        {
        }

        public ContainerNotFoundException(string message) : base(message)
        {
        }
    }
}