using System;
using System.Threading;
using System.Threading.Tasks;
using Sparrow.Server;
using Sparrow.Utils;

namespace Raven.Server.Documents;

public class IndexWaiter : IDisposable
{
    private long _lastCompletedIndex;
    private readonly AsyncManualResetEvent _notifiedListeners;

    public long LastIndex => Interlocked.Read(ref _lastCompletedIndex);

    public IndexWaiter(CancellationToken token)
    {
        _notifiedListeners = new AsyncManualResetEvent(token);
    }
    
    public void SetAndNotifyListenersIfHigher(long newIndex)
    {
        if (ThreadingHelper.InterlockedExchangeMax(ref _lastCompletedIndex, newIndex))
        {
            _notifiedListeners.SetAndResetAtomically();
        }
    }

    public void NotifyListenersAboutError(Exception e)
    {
        _notifiedListeners.SetException(e);
    }
    
    public async Task WaitAsync(long index, CancellationToken token)
    {
        while (true)
        {
            var waitAsync = _notifiedListeners.WaitAsync(token);
            long lastIndex = LastIndex;
            if (index <= lastIndex)
                break;

            var waitTask = await Task.WhenAny(waitAsync);
            if (waitTask.IsCanceled)
            {
                ThrowCanceledException(index);
            }
            await waitTask;
        }
    }
    
    public async Task WaitAsync(long index, TimeSpan timeout)
    {
        while (true)
        {
            Task waitAsync = _notifiedListeners.WaitAsync(timeout);
            long lastIndex = LastIndex;
            if (index <= lastIndex)
                break;

            var waitTask = await Task.WhenAny(waitAsync);
            if (waitTask.IsCanceled)
            {
                ThrowTimeoutException(timeout, index);
            }
            await waitTask;
        }
    }

    private void ThrowCanceledException(long index)
    {
        throw new OperationCanceledException($"Cancelled while waiting for task with index {index} to complete. Last commit index is: {LastIndex}.");
    }
    
    private void ThrowTimeoutException(TimeSpan value, long index)
    {
        throw new TimeoutException($"Waited for {value} for task with index {index} to complete. Last commit index is: {LastIndex}");
    }

    public void Dispose()
    {
        _notifiedListeners?.Dispose();
    }
}
