using System;

namespace Voron
{
    internal sealed class NonDurableFileSystemException : Exception
    {
        public NonDurableFileSystemException()
        {
        }

        public NonDurableFileSystemException(string message) : base(message)
        {
        }

        public NonDurableFileSystemException(string message, Exception innerException) : base(message, innerException)
        {
        }
    }
}