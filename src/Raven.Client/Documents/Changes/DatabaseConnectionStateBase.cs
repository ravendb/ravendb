using System;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Extensions;

namespace Raven.Client.Documents.Changes;

public abstract class DatabaseConnectionStateBase
{
    public event Action<Exception> OnError;

    private readonly Func<Task> _onDisconnect;
    public readonly Func<Task> OnConnect;
    private int _value;
    public Exception LastException;

    private readonly TaskCompletionSource<object> _firstSet = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private Task _connected;

    protected DatabaseConnectionStateBase(Func<Task> onConnect, Func<Task> onDisconnect)
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

    public virtual void Dispose()
    {
        Set(Task.FromException(new ObjectDisposedException(nameof(DatabaseConnectionState))));
        OnError = null;
    }
}
