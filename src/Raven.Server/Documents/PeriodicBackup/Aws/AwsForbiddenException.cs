using System;

namespace Raven.Server.Documents.PeriodicBackup.Aws
{
    public class AwsForbiddenException : Exception
    {
        public AwsForbiddenException()
        {

        }

        public AwsForbiddenException(string message) : base(message)
        {
        }

        public AwsForbiddenException(string message, Exception inner) : base(message, inner)
        {
        }
    }
}