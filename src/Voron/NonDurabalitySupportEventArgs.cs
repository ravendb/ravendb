namespace Voron
{
    using System;

    public class NonDurabalitySupportEventArgs : EventArgs
    {
        internal NonDurabalitySupportEventArgs(string message, Exception exception)
        {
            Message = message;
            Exception = exception;
        }

        public string Message { get; private set; }
        public Exception Exception { get; private set; }
    }
}
