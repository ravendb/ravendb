namespace Voron
{
    using System;

    public class RecoveryErrorEventArgs : EventArgs
    {
        internal RecoveryErrorEventArgs(string message, Exception exception)
        {
            Message = message;
            Exception = exception;
        }

        public string Message { get; private set; }
        public Exception Exception { get; private set; }
    }
}