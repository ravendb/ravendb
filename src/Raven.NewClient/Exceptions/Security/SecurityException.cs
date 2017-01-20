using System;

namespace Raven.NewClient.Exceptions.Security
{
    public abstract class SecurityException : Exception
    {
        protected SecurityException()
        {
        }

        protected SecurityException(string message) : base(message)
        {
        }

        protected SecurityException(string message, Exception inner) : base(message, inner)
        {
        }
    }
}