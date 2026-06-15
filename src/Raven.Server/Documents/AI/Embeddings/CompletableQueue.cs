using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using Sparrow.Server;

namespace Raven.Server.Documents.AI.Embeddings;

internal sealed class CompletableQueue<T>
{
    private readonly ConcurrentQueue<T> _queue = new();
    private readonly AsyncManualResetEvent _hasWork = new();
    private readonly object _lock = new();
    private bool _completed;

    // Intentionally does not wake readers.
    // ETL relies on enqueueing multiple items and waking once from WaitForGenerationAsync.
    // Call Wake() explicitly after enqueue when immediate processing is required.
    public bool TryEnqueue(T item)
    {
        lock (_lock)
        {
            if (_completed)
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
            _completed = true;
        }

        _hasWork.Set();
    }

    public bool TryDequeue(out T item) => _queue.TryDequeue(out item);

    public async ValueTask<bool> WaitToReadAsync(CancellationToken token = default)
    {
        while (true)
        {
            lock (_lock)
            {
                if (_completed)
                    return _queue.IsEmpty == false;
            }

            await _hasWork.WaitAsync(token);
            _hasWork.Reset();

            lock (_lock)
            {
                if (_completed)
                    return _queue.IsEmpty == false;

                if (_queue.IsEmpty == false)
                    return true;
            }
        }
    }
}
