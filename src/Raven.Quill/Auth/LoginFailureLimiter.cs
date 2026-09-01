using System.Collections.Concurrent;

namespace Raven.Quill.Auth;

public sealed class LoginFailureLimiter(TimeProvider time)
{
    internal const int MaxFailures = 10;
    internal static readonly TimeSpan Window = TimeSpan.FromMinutes(1);

    private const int MaxTrackedClients = 10_000;

    private readonly ConcurrentDictionary<string, Counter> _failures = new();
    private long _lastPurgeTicks;

    public bool IsThrottled(string client)
    {
        if (_failures.TryGetValue(client, out var counter) == false)
            return false;

        lock (counter)
        {
            return counter.Removed == false &&
                   time.GetUtcNow() - counter.WindowStart <= Window &&
                   counter.Count > MaxFailures;
        }
    }

    public bool RegisterFailure(string client)
    {
        var now = time.GetUtcNow();

        if (_failures.Count > MaxTrackedClients)
        {
            PurgeExpired(now);

            if (_failures.Count > MaxTrackedClients && _failures.ContainsKey(client) == false)
                return false;
        }

        while (true)
        {
            var counter = _failures.GetOrAdd(client, _ => new Counter { WindowStart = now });
            lock (counter)
            {
                if (counter.Removed == false)
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

            _failures.TryRemove(new KeyValuePair<string, Counter>(client, counter));
        }
    }

    public void Reset(string client)
    {
        if (_failures.TryGetValue(client, out var counter) == false)
            return;

        lock (counter)
        {
            counter.Removed = true;
            _failures.TryRemove(new KeyValuePair<string, Counter>(client, counter));
        }
    }

    private void PurgeExpired(DateTimeOffset now)
    {
        var last = Interlocked.Read(ref _lastPurgeTicks);
        if (now.UtcTicks - last < Window.Ticks)
            return;

        if (Interlocked.CompareExchange(ref _lastPurgeTicks, now.UtcTicks, last) != last)
            return;

        foreach (var (client, counter) in _failures)
        {
            lock (counter)
            {
                if (now - counter.WindowStart > Window)
                {
                    counter.Removed = true;
                    _failures.TryRemove(new KeyValuePair<string, Counter>(client, counter));
                }
            }
        }
    }

    private sealed class Counter
    {
        public DateTimeOffset WindowStart;
        public int Count;
        public bool Removed;
    }
}
