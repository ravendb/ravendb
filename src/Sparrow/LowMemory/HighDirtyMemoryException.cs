using System;

namespace Sparrow.LowMemory
{
    public sealed class HighDirtyMemoryException : Exception
    {
        public HighDirtyMemoryException()
        {
        }

        public HighDirtyMemoryException(string message) : base(message)
        {
        }

        public HighDirtyMemoryException(string message, Exception inner) : base(message, inner)
        {
        }
    }
}
