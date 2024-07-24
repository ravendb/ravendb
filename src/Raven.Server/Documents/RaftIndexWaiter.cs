using System;
using System.Threading;
using System.Threading.Tasks;
using Sparrow.Server;
using Sparrow.Utils;

namespace Raven.Server.Documents;

public class RaftIndexWaiter : IDisposable
{
    private long _lastCompletedIndex;
    private readonly AsyncManualResetEvent _notifiedListeners;

    public long LastIndex => Interlocked.Read(ref _lastCompletedIndex);

    public RaftIndexWaiter(CancellationToken token)
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
            if (index <= LastIndex)
                break;

            if (token.IsCancellationRequested)
                ThrowCanceledException(index);

            await Task.WhenAny(waitAsync);
            if(waitAsync.IsCanceled)
                ThrowCanceledException(index);

            await waitAsync;
        }
    }
    
    public async Task WaitAsync(long index, TimeSpan timeout)
    {
        while (true)
        {
            var waitAsync = _notifiedListeners.WaitAsync(timeout);
            if (index <= LastIndex)
                break;

            if (await waitAsync == false)
            {
                var copy = LastIndex;
                if (index <= copy)
                    break;

                ThrowTimeoutException(timeout, index, copy);
            }
        }
    }

    private void ThrowCanceledException(long index)
    {
        throw new OperationCanceledException($"Cancelled while waiting for task with index {index} to complete. Last commit index is: {LastIndex}.");
    }
    
    private void ThrowTimeoutException(TimeSpan value, long index, long lastIndex)
    {
        throw new TimeoutException($"Waited for {value} for task with index {index} to complete. Last commit index is: {lastIndex}");
    }

    public void Dispose()
    {
        _notifiedListeners?.Dispose();
    }
}
