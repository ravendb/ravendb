using System;

namespace Sparrow.Exceptions
{
    public sealed class MemoryInfoException : Exception
    {

        public MemoryInfoException()
        {
        }

        public MemoryInfoException(string message) : base(message)
        {
        }

        public MemoryInfoException(string message, Exception inner) : base(message, inner)
        {
        }
    }
}