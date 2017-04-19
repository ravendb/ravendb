using System;

namespace Voron
{
    internal class NonDurableFileSystemException : Exception
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