using System;

namespace Sparrow.Utils
{
    public sealed class LockAlreadyDisposedException : ObjectDisposedException
    {
        public LockAlreadyDisposedException(string message) : base(message)
        {
        }

        public LockAlreadyDisposedException(string message, Exception innerException) : base(message, innerException)
        {
        }
    }
}
