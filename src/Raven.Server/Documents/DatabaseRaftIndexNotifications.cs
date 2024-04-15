using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.ExceptionServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Server.ServerWide;
using Sparrow.Logging;
using Sparrow.Server;
using Sparrow.Utils;
using static Raven.Server.Documents.DatabasesLandlord;

namespace Raven.Server.Documents;

public class DatabaseRaftIndexNotifications : IDisposable
{
    public long LastModifiedIndex;//--protected
    private readonly AsyncManualResetEvent _notifiedListeners;//--protected
    private readonly ConcurrentQueue<ErrorHolder> _errors = new ConcurrentQueue<ErrorHolder>();//--protected
    private int _numberOfErrors;//--protected
    // private readonly ConcurrentDictionary<long, TaskCompletionSource<object>> _tasksDictionary = new ConcurrentDictionary<long, TaskCompletionSource<object>>();

    private readonly Queue<DatabaseNotification> _recentNotifications = new Queue<DatabaseNotification>();
    internal Logger Log; //--protected
    // private SingleUseFlag _isDisposed = new SingleUseFlag();

    private readonly RachisLogIndexNotifications _clusterStateMachineLogIndexNotifications;

    private class ErrorHolder
    {
        public long Index;
        public ExceptionDispatchInfo Exception;
    }

    public DatabaseRaftIndexNotifications(RachisLogIndexNotifications clusterStateMachineLogIndexNotifications, CancellationToken token)
    {
        _notifiedListeners = new AsyncManualResetEvent(token);
        _clusterStateMachineLogIndexNotifications = clusterStateMachineLogIndexNotifications;
    }

    public void Dispose()
    {
        // _isDisposed.Raise();
        _notifiedListeners.Dispose();
        // foreach (var task in _tasksDictionary.Values)
        // {
        //     task.TrySetCanceled();
        // }
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

    private string PrintLastNotifications()
    {
        var notifications = _recentNotifications.ToArray();
        var builder = new StringBuilder(notifications.Length);
        foreach (var notification in notifications)
        {
            builder
                .Append("Index: ")
                .Append(notification.Index)
                .Append(". Type: ")
                .Append(notification.Type)
                .Append(". Exception: ")
                .Append(notification.Exception)
                .AppendLine();
        }
        return builder.ToString();
    }

    public void RecordNotification(DatabaseNotification notification)
    {
        _recentNotifications.Enqueue(notification);
        while (_recentNotifications.Count > 50)
            _recentNotifications.TryDequeue(out _);
    }

    public void NotifyListenersAbout(DatabaseNotification notification)
    {
        RecordNotification(notification);

        long index = notification.Index;
        Exception e = notification.Exception;

        if (e != null)
        {
            _errors.Enqueue(new ErrorHolder
            {
                Index = index,
                Exception = ExceptionDispatchInfo.Capture(e)
            });

            if (Interlocked.Increment(ref _numberOfErrors) > 50)
            {
                _errors.TryDequeue(out _);
                Interlocked.Decrement(ref _numberOfErrors);
            }
        }

        ThreadingHelper.InterlockedExchangeMax(ref LastModifiedIndex, index);
        _notifiedListeners.SetAndResetAtomically();
    }

    public ConcurrentDictionary<long, TaskCompletionSource<object>> GetTasksDictionary()
    {
        return _clusterStateMachineLogIndexNotifications.GetTasksDictionary();
    }
}

public class DatabaseNotification
{
    public long Index;
    public Exception Exception;
    public DatabaseNotificationChangeType Type;
}

public enum DatabaseNotificationChangeType
{
    StateChanged,
    ValueChanged,
    PendingClusterTransactions,
    ClusterTransactionCompleted,

    IndexStart,
    IndexUpdateSorters,
    IndexUpdateAnalyzers,
    AutoIndexStart,
    UpdateStaticIndex,
    DeleteIndex
}
