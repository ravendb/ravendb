using System;
using System.Collections.Generic;
using System.Text;

namespace Sparrow.LowMemory
{
    public class HighDirtyMemoryException : Exception
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
