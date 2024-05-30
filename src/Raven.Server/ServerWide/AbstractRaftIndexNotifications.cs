using System;
using System.Collections.Concurrent;
using System.Runtime.ExceptionServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Server.Documents;
using Sparrow.Utils;

namespace Raven.Server.ServerWide;

public abstract class AbstractRaftIndexNotifications<TNotification> : IDisposable
where TNotification : RaftIndexNotification
{
    protected readonly ConcurrentQueue<ErrorHolder> Errors = new ConcurrentQueue<ErrorHolder>();
    private readonly ConcurrentQueue<TNotification> _recentNotifications = new ConcurrentQueue<TNotification>();
    private int _numberOfErrors;
    private readonly IndexWaiter _raftIndexWaiter;

    public long LastModifiedIndex => _raftIndexWaiter.LastIndex;

    protected AbstractRaftIndexNotifications(CancellationToken token)
    {
        _raftIndexWaiter = new IndexWaiter(token);
    }

    public virtual void Dispose()
    {
        _raftIndexWaiter.Dispose();
    }

    public async Task WaitForIndexNotification(long index, CancellationToken token)
    {
        var indexWaitTask = await Task.WhenAny(_raftIndexWaiter.WaitAsync(index, token));
        if (indexWaitTask.IsCanceled)
        {
            ThrowCanceledException(index, _raftIndexWaiter.LastIndex);
        }
        await indexWaitTask;

        var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using (token.Register(() => tcs.TrySetResult()))
        {
            if (await WaitForTaskCompletion(index, new Lazy<Task>(tcs.Task)))
                return;

            ThrowCanceledException(index, _raftIndexWaiter.LastIndex, isExecution: true);
        }
    }

    public async Task WaitForIndexNotification(long index, TimeSpan timeout)
    {
        var indexWaitTask = await Task.WhenAny(_raftIndexWaiter.WaitAsync(index, timeout));
        if (indexWaitTask.IsCanceled)
        {
            ThrowTimeoutException(timeout, index, _raftIndexWaiter.LastIndex);
        }
        await indexWaitTask;

        if (await WaitForTaskCompletion(index, new Lazy<Task>(TimeoutManager.WaitFor(timeout))))
            return;

        ThrowTimeoutException(timeout, index, _raftIndexWaiter.LastIndex, isExecution: true);
    }

    public abstract Task<bool> WaitForTaskCompletion(long index, Lazy<Task> waitingTask);

    public virtual void NotifyListenersAbout(long index, Exception e)
    {
        if (e != null)
        {
            Errors.Enqueue(new ErrorHolder
            {
                Index = index,
                Exception = ExceptionDispatchInfo.Capture(e)
            });

            if (Interlocked.Increment(ref _numberOfErrors) > 25)
            {
                Errors.TryDequeue(out _);
                Interlocked.Decrement(ref _numberOfErrors);
            }
        }

        _raftIndexWaiter.SetAndNotifyListenersIfHigher(index);
    }

    public void RecordNotification(TNotification notification)
    {
        _recentNotifications.Enqueue(notification);
        while (_recentNotifications.Count > 50)
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

    private string PrintLastNotifications()
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
