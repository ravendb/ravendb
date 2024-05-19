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
            Task waitAsync = _notifiedListeners.WaitAsync(token);
            long lastIndex = LastIndex;
            if (index <= lastIndex)
                break;

            try
            {
                await waitAsync;
            }
            catch (TaskCanceledException)
            {
                ThrowCanceledException(index, lastIndex);
            }
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

            try
            {
                await waitAsync;
            }
            catch (TaskCanceledException)
            {
                ThrowTimeoutException(index, lastIndex);
            }
        }
    }

    private static void ThrowCanceledException(long index, long lastModifiedIndex)
    {
        throw new OperationCanceledException($"Cancelled while waiting to get an index notification for {index}. lastModifiedIndex {lastModifiedIndex}");
    }
    private static void ThrowTimeoutException(long index, long lastModifiedIndex)
    {
        throw new TimeoutException($"Timeout while waiting to get an index notification for {index}. lastModifiedIndex {lastModifiedIndex}");
    }

    public void Dispose()
    {
        _notifiedListeners?.Dispose();
    }
}
