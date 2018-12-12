using System;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations;
using Raven.Client.Extensions;

namespace Raven.Client.Documents.Changes
{
    internal class DatabaseConnectionState : IChangesConnectionState<DocumentChange>, IChangesConnectionState<IndexChange>, IChangesConnectionState<OperationStatusChange>, IChangesConnectionState<CounterChange>
    {
        public event Action<Exception> OnError;
        private readonly Func<Task> _onDisconnect;
        public readonly Func<Task> OnConnect;
        private int _value;
        public Exception LastException;

        private readonly TaskCompletionSource<object> _firstSet = new TaskCompletionSource<object>(TaskCreationOptions.RunContinuationsAsynchronously);
        private Task _connected;

        public void Set(Task connection)
        {
            if (_firstSet.Task.IsCompleted == false)
            {
                var task =_firstSet.Task.IgnoreUnobservedExceptions();

                connection.ContinueWith(t =>
                {
                    if (t.IsFaulted)
                        _firstSet.TrySetException(t.Exception);
                    else if (t.IsCanceled)
                        _firstSet.TrySetCanceled();
                    else
                        _firstSet.TrySetResult(null);
                });
            }
            _connected = connection;
        }

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
            return _connected ?? _firstSet.Task;
        }

        event Action<CounterChange> IChangesConnectionState<CounterChange>.OnChangeNotification
        {
            add => OnCounterChangeNotification += value;
            remove => OnCounterChangeNotification -= value;
        }

        event Action<OperationStatusChange> IChangesConnectionState<OperationStatusChange>.OnChangeNotification
        {
            add => OnOperationStatusChangeNotification += value;
            remove => OnOperationStatusChangeNotification -= value;
        }

        event Action<IndexChange> IChangesConnectionState<IndexChange>.OnChangeNotification
        {
            add => OnIndexChangeNotification += value;
            remove => OnIndexChangeNotification -= value;
        }

        event Action<DocumentChange> IChangesConnectionState<DocumentChange>.OnChangeNotification
        {
            add => OnDocumentChangeNotification += value;
            remove => OnDocumentChangeNotification -= value;
        }

        public void Dispose()
        {
            Set(Task.FromException(new ObjectDisposedException(nameof(DatabaseConnectionState))));
            OnDocumentChangeNotification = null;
            OnIndexChangeNotification = null;
            OnOperationStatusChangeNotification = null;
            OnError = null;
        }
        
        public DatabaseConnectionState( Func<Task> onConnect, Func<Task> onDisconnect)
        {
            OnConnect = onConnect;
            _onDisconnect = onDisconnect;
            _value = 0;
        }

        private event Action<DocumentChange> OnDocumentChangeNotification;

        private event Action<CounterChange> OnCounterChangeNotification;

        private event Action<IndexChange> OnIndexChangeNotification;

        private event Action<OperationStatusChange> OnOperationStatusChangeNotification;

        public void Send(DocumentChange documentChange)
        {
            OnDocumentChangeNotification?.Invoke(documentChange);
        }

        public void Send(CounterChange counterChange)
        {
            OnCounterChangeNotification?.Invoke(counterChange);
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
