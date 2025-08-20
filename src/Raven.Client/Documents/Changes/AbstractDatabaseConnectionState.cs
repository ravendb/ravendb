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

    private readonly TaskCompletionSource<object> _firstSet = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private Task _connected;

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
                var tcs = (TaskCompletionSource<object>)firstSet;

                if (t.IsFaulted)
                    tcs.TrySetException(t.Exception);
                else if (t.IsCanceled)
                    tcs.TrySetCanceled();
                else
                    tcs.TrySetResult(null);
            }, _firstSet);
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
        if(_connected?.Exception == null)
            Set(Task.FromException(new ObjectDisposedException(nameof(DatabaseConnectionState))));
        OnError = null;
    }
}
