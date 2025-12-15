using System;
using System.Threading.Tasks;

namespace Raven.Client.Documents.Changes
{
    internal interface IChangesConnectionState<out T> : IDisposable
    {
        int Inc();

        int Dec();

        void Error(Exception e);

        Task EnsureSubscribedNow();

        void RegisterEvents(Action<T> onChangeNotification, Action<Exception> onError);

        void UnregisterEvents(Action<T> onChangeNotification, Action<Exception> onError);

        event Action<Exception> OnError;
    }
}
