using System;

namespace Raven.Server.Exceptions
{
    public class PendingRollingIndexException : Exception
    {
        public PendingRollingIndexException()
        {

        }

        public PendingRollingIndexException(string message) : base(message)
        {

        }
    }
}
