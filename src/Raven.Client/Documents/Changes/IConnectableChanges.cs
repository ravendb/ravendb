using System;

namespace Raven.Client.Documents.Changes
{
    public interface IConnectableChanges : IDisposable
    {
        bool Connected { get; }

        event EventHandler ConnectionStatusChanged;

        event Action<Exception> OnError;
    }
}
