using System;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations;

namespace Raven.Client.Documents.Changes
{
    internal class DatabaseConnectionState : IChangesConnectionState
    {
        private readonly SemaphoreSlim _semaphore = new SemaphoreSlim(1, 1);

        public event Action<Exception> OnError;
        private readonly Func<Task> _onDisconnect;
        private readonly IDatabaseChanges _changes;
        public readonly Func<Task> OnConnect;
        private int _value;
        public Exception LastException;

        private Task _connected;

        public void Set(Task connection) => _connected = connection;

        public void Inc()
        {
            Interlocked.Increment(ref _value);
        }

        public void Dec()
        {
            if (Interlocked.Decrement(ref _value) == 0)
            {
                Set(_onDisconnect());
            }
        }

        public void Error(Exception e)
        {
            Set(Task.FromException(e));
            LastException = e;
            OnError?.Invoke(e);
        }

        public Task EnsureSubscribedNow()
        {
            return _connected;
        }

        public void Dispose()
        {
            Set(Task.FromCanceled(CancellationToken.None));
        }
        
        public DatabaseConnectionState(IDatabaseChanges changes, Func<Task> onConnect, Func<Task> onDisconnect)
        {
            _changes = changes;
            OnConnect = onConnect;
            _onDisconnect = onDisconnect;
            _value = 0;
        }

        public event Action<DocumentChange> OnDocumentChangeNotification;

        public event Action<IndexChange> OnIndexChangeNotification;

        public event Action<OperationStatusChange> OnOperationStatusChangeNotification;

        public void Send(DocumentChange documentChange)
        {
            OnDocumentChangeNotification?.Invoke(documentChange);
        }

        public void Send(IndexChange indexChange)
        {
            OnIndexChangeNotification?.Invoke(indexChange);
        }

        public void Send(OperationStatusChange operationStatusChange)
        {
            OnOperationStatusChangeNotification?.Invoke(operationStatusChange);
        }
    }
}
