namespace Voron
{
    using System;

    public class RecoveryErrorEventArgs : EventArgs
    {
        internal RecoveryErrorEventArgs(string message)
        {
            Message = message;
        }

        public string Message { get; private set; }
    }
}