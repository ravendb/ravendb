using System;

namespace Voron
{
    public class DataIntegrityErrorEventArgs : EventArgs
    {
        internal DataIntegrityErrorEventArgs(string message, Exception exception)
        {
            Message = message;
            Exception = exception;
        }

        public string Message { get; private set; }
        public Exception Exception { get; private set; }
    }
}
