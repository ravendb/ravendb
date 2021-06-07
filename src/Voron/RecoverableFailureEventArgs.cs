using System;

namespace Voron
{
    public class RecoverableFailureEventArgs : EventArgs
    {
        internal RecoverableFailureEventArgs(string failureMessage, Guid environmentId, string environmentPath, Exception exception)
        {
            FailureMessage = failureMessage;
            EnvironmentId = environmentId;
            EnvironmentPath = environmentPath;
            Exception = exception;
        }

        public string FailureMessage { get; }

        public Guid EnvironmentId { get; }

        public string EnvironmentPath { get; }

        public Exception Exception { get; }
    }
}
