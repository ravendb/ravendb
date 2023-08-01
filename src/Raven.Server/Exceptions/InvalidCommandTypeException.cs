using System;

namespace Raven.Server.Exceptions
{
    public sealed class InvalidCommandTypeException : Exception
    {
        public InvalidCommandTypeException(string msg): base(msg) { }
    }
}
