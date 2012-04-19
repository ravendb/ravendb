using System;

namespace Raven.Studio.Infrastructure
{
    public interface INotifyOnDataFetchErrors
    {
        event EventHandler<DataFetchErrorEventArgs> DataFetchError;
        event EventHandler<EventArgs> FetchSucceeded;
        void Retry();
    }
}
