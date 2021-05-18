using System;

namespace Raven.Client.Exceptions
{
    public class PendingRollingIndexException : RavenException
    {
        public PendingRollingIndexException()
        {

        }

        public PendingRollingIndexException(string message) : base(message)
        {

        }
    }
}
