using System;

namespace Raven.Client.Exceptions.Security
{
    public abstract class SecurityException : RavenException
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