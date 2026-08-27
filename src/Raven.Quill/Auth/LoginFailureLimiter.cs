using System.Collections.Concurrent;

namespace Raven.Quill.Auth;

public sealed class LoginFailureLimiter(TimeProvider time)
{
    internal const int MaxFailures = 10;
    internal static readonly TimeSpan Window = TimeSpan.FromMinutes(1);

    private const int MaxTrackedClients = 10_000;

    private readonly ConcurrentDictionary<string, Counter> _failures = new();

    public bool IsThrottled(string client)
    {
        if (_failures.TryGetValue(client, out var counter) == false)
            return false;

        lock (counter)
        {
            return time.GetUtcNow() - counter.WindowStart <= Window && counter.Count > MaxFailures;
        }
    }

    public bool RegisterFailure(string client)
    {
        var now = time.GetUtcNow();

        if (_failures.Count > MaxTrackedClients)
            PurgeExpired(now);

        var counter = _failures.GetOrAdd(client, _ => new Counter { WindowStart = now });
        lock (counter)
        {
            if (now - counter.WindowStart > Window)
            {
                counter.WindowStart = now;
                counter.Count = 0;
            }

            counter.Count++;
            return counter.Count > MaxFailures;
        }
    }

    public void Reset(string client) => _failures.TryRemove(client, out _);

    private void PurgeExpired(DateTimeOffset now)
    {
        foreach (var (client, counter) in _failures)
        {
            lock (counter)
            {
                if (now - counter.WindowStart > Window)
                    _failures.TryRemove(client, out _);
            }
        }
    }

    private sealed class Counter
    {
        public DateTimeOffset WindowStart;
        public int Count;
    }
}
