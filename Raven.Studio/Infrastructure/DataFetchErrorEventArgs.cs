using System;

namespace Raven.Studio.Infrastructure
{
    public class DataFetchErrorEventArgs : EventArgs
    {
        public Exception Error { get; private set; }

        public DataFetchErrorEventArgs(Exception error)
        {
            Error = error;
        }
    }
}
