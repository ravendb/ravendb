using System;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Extensions;

namespace Raven.Client.Documents.Changes;

internal abstract class AbstractDatabaseConnectionState
{
    public event Action<Exception> OnError;

    private readonly Func<Task> _onDisconnect;
    public readonly Func<Task> OnConnect;
    private int _value;
    public Exception LastException;

    private readonly TaskCompletionSource _firstSet = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private Task _connected;
    protected readonly object _eventLock = new object();

    protected AbstractDatabaseConnectionState(Func<Task> onConnect, Func<Task> onDisconnect)
    {
        OnConnect = onConnect;
        _onDisconnect = onDisconnect;
        _value = 0;
    }

    public void Set(Task connection)
    {
        if (_firstSet.Task.IsCompleted == false)
        {
            var task = _firstSet.Task.IgnoreUnobservedExceptions();

            connection.ContinueWith(static (t, firstSet) =>
            {
                var tcs = (TaskCompletionSource)firstSet;

                if (t.IsFaulted)
                    tcs.TrySetException(t.Exception);
                else if (t.IsCanceled)
                    tcs.TrySetCanceled();
                else
                    tcs.TrySetResult();
            }, _firstSet);
        }
        _connected = connection;
    }

    public int Inc()
    {
        return Interlocked.Increment(ref _value);
    }

    public int Dec()
    {
        var val = Interlocked.Decrement(ref _value);
        if (val == 0)
        {
            Set(_onDisconnect());
        }
        return val;
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

    protected void CallEventInternal<T>(Action<T> changeHandler, T change)
    {
        Action<T> handler;
        lock (_eventLock)
        {
            handler = changeHandler;
        }

        handler?.Invoke(change);
    }

    protected void RegisterEventsInternal<T>(ref Action<T> onChangeNotification, Action<T> changeHandler, Action<Exception> errorHandler)
    {
        lock (_eventLock)
        {
            if (_disposed)
                return;

            onChangeNotification += changeHandler;
            OnError += errorHandler;
        }
    }

    protected void UnregisterEventsInternal<T>(ref Action<T> onChangeNotification, Action<T> changeHandler, Action<Exception> errorHandler)
    {
        lock (_eventLock)
        {
            if (_disposed)  // already disposed
                return;

            onChangeNotification -= changeHandler;
            OnError -= errorHandler;
        }
    }

    private volatile bool _disposed;

    public virtual void Dispose()
    {
        lock (_eventLock)
        {
            if (_disposed)
                return;

            _disposed = true;
            if (_connected?.Exception == null)
                Set(Task.FromException(new ObjectDisposedException(nameof(DatabaseConnectionState))));

            OnError = null;
        }
    }
}
