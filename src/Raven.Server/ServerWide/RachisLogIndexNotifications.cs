using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Sparrow.Logging;
using Sparrow.Threading;

namespace Raven.Server.ServerWide;

public class RachisLogIndexNotifications : AbstractRaftIndexNotifications<RecentLogIndexNotification>
{
    public Logger Log;

    private readonly ConcurrentDictionary<long, TaskCompletionSource<object>> _tasksDictionary =
        new ConcurrentDictionary<long, TaskCompletionSource<object>>();

    private SingleUseFlag _isDisposed = new SingleUseFlag();

    public RachisLogIndexNotifications(CancellationToken token) : base(token)
    {
    }

    public override void Dispose()
    {
        _isDisposed.Raise();
        base.Dispose();
        foreach (var task in _tasksDictionary.Values)
        {
            task.TrySetCanceled();
        }
    }

    public override async Task<bool> WaitForTaskCompletion(long index, Lazy<Task> waitingTask)
    {
        if (_tasksDictionary.TryGetValue(index, out var tcs) == false)
        {
            // the task has already completed
            // let's check if we had errors in it
            foreach (var error in Errors)
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

    private void SetTaskCompleted(long index, Exception e)
    {
        if (_tasksDictionary.TryGetValue(index, out var tcs))
        {
            // set the task as finished
            if (e == null)
            {
                if (tcs.TrySetResult(null) == false)
                    LogFailureToSetTaskResult();
            }
            else
            {
                if (tcs.TrySetException(e) == false)
                    LogFailureToSetTaskResult();
            }

            void LogFailureToSetTaskResult()
            {
                if (Log.IsInfoEnabled)
                    Log.Info($"Failed to set result of task with index {index}");
            }
        }

        _tasksDictionary.TryRemove(index, out _);
    }

    public override void NotifyListenersAbout(long index, Exception e)
    {
        SetTaskCompleted(index, e);
        base.NotifyListenersAbout(index, e);
    }

    public void AddTask(long index)
    {
        Debug.Assert(_tasksDictionary.TryGetValue(index, out _) == false, $"{nameof(_tasksDictionary)} should not contain task with key {index}");
        if (_isDisposed.IsRaised())
            throw new ObjectDisposedException(nameof(RachisLogIndexNotifications));

        _tasksDictionary.TryAdd(index, new TaskCompletionSource<object>(TaskCreationOptions.RunContinuationsAsynchronously));
    }
}

public class RecentLogIndexNotification : RaftIndexNotification
{
    public string Type;
    public TimeSpan ExecutionTime;
    public int? LeaderErrorCount;
    public long? Term;
    public long? LeaderShipDuration;

    public override string ToString()
    {
        return
            $"Index: {Index}. Type: {Type}. ExecutionTime: {ExecutionTime}. Term: {Term}. LeaderErrorCount: {LeaderErrorCount}. LeaderShipDuration: {LeaderShipDuration}. Exception: {Exception}";
    }
}
