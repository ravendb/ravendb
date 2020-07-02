using System;

namespace Raven.Server.Exceptions
{
    public class InvalidCommandTypeException : Exception
    {
        public InvalidCommandTypeException(string msg): base(msg) { }
    }
}
