using System;

namespace Raven.Studio.Infrastructure
{
    public interface INotifyOnDataFetchErrors
    {
        event EventHandler<DataFetchErrorEventArgs> DataFetchError;
        event EventHandler<EventArgs> FetchStarting;
        event EventHandler<EventArgs> FetchCompleted;
        void Retry();
    }
}
