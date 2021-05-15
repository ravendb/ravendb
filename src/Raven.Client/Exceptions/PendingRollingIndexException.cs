using System;

namespace Raven.Client.Exceptions
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
