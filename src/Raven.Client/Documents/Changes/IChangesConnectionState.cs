using System;
using System.Threading.Tasks;

namespace Raven.Client.Documents.Changes
{
    internal interface IChangesConnectionState<out T> : IDisposable
    {
        void Inc();

        void Dec();

        void Error(Exception e);

        Task EnsureSubscribedNow();

        event Action<T> OnChangeNotification;

        event Action<Exception> OnError;
    }
}
