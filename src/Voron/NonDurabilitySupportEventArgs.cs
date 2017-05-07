namespace Voron
{
    using System;

    public class NonDurabilitySupportEventArgs : EventArgs
    {
        internal NonDurabilitySupportEventArgs(string message, Exception exception, string details) 
        {
            Message = message;
            Exception = exception;
            Details = details;
        }

        public string Message { get; private set; }
        public Exception Exception { get; private set; }
        public string Details { get; private set; }
    }
}
