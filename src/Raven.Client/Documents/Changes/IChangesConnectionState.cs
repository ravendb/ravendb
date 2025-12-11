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

        event Action<T> OnChangeNotification;

        event Action<Exception> OnError;
    }
}
