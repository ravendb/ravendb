using System;
using System.Collections.Concurrent;
using System.Runtime.ExceptionServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Sparrow.Server;
using Sparrow.Utils;

namespace Raven.Server.ServerWide;

public abstract class AbstractRaftIndexNotifications<TNotification> : IDisposable
where TNotification : RaftIndexNotification
{
    public long LastModifiedIndex;
    protected readonly ConcurrentQueue<ErrorHolder> _errors = new ConcurrentQueue<ErrorHolder>();
    private readonly ConcurrentQueue<TNotification> _recentNotifications = new ConcurrentQueue<TNotification>();
    public int RecentNotificationsMaxEntries = 50;
    private readonly AsyncManualResetEvent _notifiedListeners;
    private int _numberOfErrors;

    protected AbstractRaftIndexNotifications(CancellationToken token)
    {
        _notifiedListeners = new AsyncManualResetEvent(token);
    }

    public virtual void Dispose()
    {
        _notifiedListeners.Dispose();
    }

    public async Task WaitForIndexNotification(long index, CancellationToken token)
    {
        while (true)
        {
            // first get the task, then wait on it
            var waitAsync = _notifiedListeners.WaitAsync(token);

            if (index <= Interlocked.Read(ref LastModifiedIndex))
                break;

            if (token.IsCancellationRequested)
                ThrowCanceledException(index, LastModifiedIndex, isExecution: false);

            if (await waitAsync == false)
            {
                var copy = Interlocked.Read(ref LastModifiedIndex);
                if (index <= copy)
                    break;
            }
        }

        var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using (token.Register(() => tcs.TrySetResult()))
        {
            if (await WaitForTaskCompletion(index, new Lazy<Task>(tcs.Task)))
                return;

            ThrowCanceledException(index, LastModifiedIndex, isExecution: true);
        }
    }

    public async Task WaitForIndexNotification(long index, TimeSpan timeout)
    {
        while (true)
        {
            // first get the task, then wait on it
            var waitAsync = _notifiedListeners.WaitAsync(timeout);

            if (index <= Interlocked.Read(ref LastModifiedIndex))
                break;

            if (await waitAsync == false)
            {
                var copy = Interlocked.Read(ref LastModifiedIndex);
                if (index <= copy)
                    break;

                ThrowTimeoutException(timeout, index, copy);
            }
        }

        if (await WaitForTaskCompletion(index, new Lazy<Task>(TimeoutManager.WaitFor(timeout))))
            return;

        ThrowTimeoutException(timeout, index, LastModifiedIndex, isExecution: true);
    }

    public abstract Task<bool> WaitForTaskCompletion(long index, Lazy<Task> waitingTask);

    public virtual void NotifyListenersAbout(long index, Exception e)
    {
        if (e != null)
        {
            _errors.Enqueue(new ErrorHolder
            {
                Index = index,
                Exception = ExceptionDispatchInfo.Capture(e)
            });

            if (Interlocked.Increment(ref _numberOfErrors) > 25)
            {
                _errors.TryDequeue(out _);
                Interlocked.Decrement(ref _numberOfErrors);
            }
        }

        ThreadingHelper.InterlockedExchangeMax(ref LastModifiedIndex, index);
        _notifiedListeners.SetAndResetAtomically();
    }

    public void RecordNotification(TNotification notification)
    {
        _recentNotifications.Enqueue(notification);
        while (_recentNotifications.Count > RecentNotificationsMaxEntries)
            _recentNotifications.TryDequeue(out _);
    }

    protected void ThrowCanceledException(long index, long lastModifiedIndex, bool isExecution = false)
    {
        var openingString = isExecution
            ? $"Cancelled while waiting for task with index {index} to complete. "
            : $"Cancelled while waiting to get an index notification for {index}. ";

        var closingString = isExecution
            ? string.Empty
            : Environment.NewLine +
              PrintLastNotifications();

        throw new OperationCanceledException(openingString +
                                             $"Last commit index is: {lastModifiedIndex}. " +
                                             $"Number of errors is: {_numberOfErrors}." + closingString);
    }
    
    protected void ThrowApplyException(long index, Exception e)
    {
        throw new InvalidOperationException($"Index {index} was successfully committed, but the apply failed.", e);
    }

    private void ThrowTimeoutException(TimeSpan value, long index, long lastModifiedIndex, bool isExecution = false)
    {
        var openingString = isExecution
            ? $"Waited for {value} for task with index {index} to complete. "
            : $"Waited for {value} but didn't get an index notification for {index}. ";

        var closingString = isExecution
            ? string.Empty
            : Environment.NewLine +
              PrintLastNotifications();

        throw new TimeoutException(openingString +
                                   $"Last commit index is: {lastModifiedIndex}. " +
                                   $"Number of errors is: {_numberOfErrors}." + closingString);
    }

    internal string PrintLastNotifications()
    {
        var notifications = _recentNotifications.ToArray();
        var builder = new StringBuilder(notifications.Length);
        foreach (var notification in notifications)
        {
            builder
                .Append(notification.ToString())
                .AppendLine();
        }
        return builder.ToString();
    }
}

public class ErrorHolder
{
    public long Index;
    public ExceptionDispatchInfo Exception;
}

public class RaftIndexNotification
{
    public long Index;
    public Exception Exception;

    public override string ToString()
    {
        return $"Index: {Index}. Exception: {Exception}";
    }
}
