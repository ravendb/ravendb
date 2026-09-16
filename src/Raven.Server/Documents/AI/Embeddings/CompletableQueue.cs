using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using Sparrow.Server;

namespace Raven.Server.Documents.AI.Embeddings;

internal sealed class CompletableQueue<T> : IDisposable
{
    private readonly ConcurrentQueue<T> _queue = new();
    private readonly AsyncManualResetEvent _hasWork;
    private readonly object _lock = new();
    private readonly CancellationTokenSource _completed;

    public CompletableQueue()
    {
        _completed = new CancellationTokenSource();
        _hasWork = new AsyncManualResetEvent(_completed.Token);
    }

    public bool TryEnqueue(T item)
    {
        lock (_lock)
        {
            if (_completed.IsCancellationRequested)
                return false;

            _queue.Enqueue(item);
            return true;
        }
    }

    public void Wake() => _hasWork.Set();

    public void Complete()
    {
        lock (_lock)
        {
            if (_completed.IsCancellationRequested)
                return;

            _completed.Cancel();
        }

        _hasWork.Set();
    }

    public bool TryDequeue(out T item) => _queue.TryDequeue(out item);

    public async ValueTask<bool> WaitToReadAsync(CancellationToken token = default)
    {
        while (true)
        {
            if (IsCompleted())
                return _queue.IsEmpty == false;

            try
            {
                await _hasWork.WaitAsync(token);
            }

            catch (OperationCanceledException) when (IsCompleted())
            {
                return _queue.IsEmpty == false;
            }

            _hasWork.Reset();

            if (_queue.IsEmpty == false)
                return true;
        }
    }

    private bool IsCompleted()
    {
        // ReSharper disable once InconsistentlySynchronizedField
        return _completed.IsCancellationRequested;
    }

    public void Dispose()
    {
        _hasWork.Dispose();
        _completed.Dispose();
    }
}
