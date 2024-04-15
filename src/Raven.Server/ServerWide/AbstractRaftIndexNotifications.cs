using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.ExceptionServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Sparrow.Logging;
using Sparrow.Server;
using Sparrow.Utils;

namespace Raven.Server.ServerWide;

public abstract class AbstractRaftIndexNotifications : IDisposable
{
    public long LastModifiedIndex;
    private readonly AsyncManualResetEvent _notifiedListeners;
    private readonly ConcurrentQueue<ErrorHolder> _errors = new ConcurrentQueue<ErrorHolder>();
    private int _numberOfErrors;

    protected AbstractRaftIndexNotifications(CancellationToken token)
    {
        _notifiedListeners = new AsyncManualResetEvent(token);
    }

    public async Task WaitForIndexNotification(long index, CancellationToken token)
    {
        Task<bool> waitAsync;
        while (true)
        {
            // first get the task, then wait on it
            waitAsync = _notifiedListeners.WaitAsync(token);

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

        if (await WaitForTaskCompletion(index, new Lazy<Task>(waitAsync)))
            return;

        ThrowCanceledException(index, LastModifiedIndex, isExecution: true);
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

    private async Task<bool> WaitForTaskCompletion(long index, Lazy<Task> waitingTask)
    {
        if (GetTasksDictionary().TryGetValue(index, out var tcs) == false)
        {
            // the task has already completed
            // let's check if we had errors in it
            foreach (var error in _errors)
            {
                if (error.Index == index)
                    error.Exception.Throw(); // rethrow
            }

            return true;
        }

        var task = tcs.Task;

        if (task.IsCompleted)
        {
            if (task.IsFaulted)
            {
                try
                {
                    await task; // will throw on error
                }
                catch (Exception e)
                {
                    ThrowApplyException(index, e);
                }
            }

            if (task.IsCanceled)
                ThrowCanceledException(index, LastModifiedIndex);

            return true;
        }

        var result = await Task.WhenAny(task, waitingTask.Value);

        if (result.IsFaulted)
            await result; // will throw

        if (task.IsCanceled)
            ThrowCanceledException(index, LastModifiedIndex);

        if (result == task)
            return true;

        return false;
    }

    private void ThrowCanceledException(long index, long lastModifiedIndex, bool isExecution = false)
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

    private void ThrowApplyException(long index, Exception e)
    {
        throw new InvalidOperationException($"Index {index} was successfully committed, but the apply failed.", e);
    }

    protected void NotifyListenersInternal(long index, Exception e)
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

    protected abstract string PrintLastNotifications();

    internal abstract ConcurrentDictionary<long, TaskCompletionSource<object>> GetTasksDictionary();

    public virtual void Dispose()
    {
        _notifiedListeners.Dispose();
    }
}

public class ErrorHolder
{
    public long Index;
    public ExceptionDispatchInfo Exception;
}
