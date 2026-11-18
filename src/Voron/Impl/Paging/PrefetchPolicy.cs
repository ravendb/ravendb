using System;
using System.Threading;
using Sparrow;
using Sparrow.LowMemory;
using Sparrow.Server.LowMemory;
using Voron.Impl.Journal;

namespace Voron.Impl.Paging;

/// <summary>
/// Reads ahead each touched segment once per warm-up. A warm-up ends below <see cref="StopPercent"/>% free memory
/// (MemFree on Linux) and a new one starts above <see cref="StartPercent"/>%.
/// </summary>
public sealed class PrefetchPolicy : IDisposable
{
    public const int StopPercent = 5;
    public const int StartPercent = 10;
    private const int TickMs = 1000;

    public static readonly PrefetchPolicy Global = new(MemoryInformation.TotalPhysicalMemory.GetValue(SizeUnit.Bytes), startTimer: true);

    private readonly long _endWarmUpBelowBytes;
    private readonly long _startWarmUpAboveBytes;
    private volatile byte _generation;
    private byte _lastGeneration;
    private int _clock;
    private int _staleAfterSeconds = int.MaxValue;
    private const int StaleAfterSecondsWithoutEvictions = 60;
    private const int EvictionSamples = 10;
    private readonly long[] _evicted = new long[EvictionSamples];
    private int _evictedCount;
    private readonly PageCacheEvictions _evictions;
    private readonly Timer _timer;

    public PrefetchPolicy(long totalMemoryBytes, bool startTimer)
    {
        _endWarmUpBelowBytes = totalMemoryBytes / 100 * StopPercent;
        _startWarmUpAboveBytes = totalMemoryBytes / 100 * StartPercent;
        if (startTimer == false)
            return;

        _evictions = PageCacheEvictions.TryCreate();
        if (_evictions == null)
            _staleAfterSeconds = StaleAfterSecondsWithoutEvictions; // Windows and macOS report no evictions
        // free memory reads as zero until the server's low-memory monitor runs: no warm-up without it
        _timer = new Timer(_ =>
        {
            Tick(LowMemoryNotification.Instance.LastMemoryInfo.AvailableMemory.GetValue(SizeUnit.Bytes));
            if (_evictions?.TryRead(out var evicted, out var filePages) == true)
                AddEvictions(evicted, filePages);
            DeviceWriteBudget.Tick();
        }, null, 0, TickMs);
    }

    /// <summary>1 to 255 during a warm-up, 0 when memory is full.</summary>
    public byte Generation => _generation;

    // Page hints age by this clock (seconds). A hint is stale after half the time the page cache takes to turn over at the
    // eviction rate of the last 10 ticks, so a page that left the cache is hinted again; never while nothing is evicted.
    public int Clock => Volatile.Read(ref _clock);

    public int StaleAfterSeconds => Volatile.Read(ref _staleAfterSeconds);

    internal void AddEvictions(long evictedFilePages, long filePages)
    {
        _evicted[_evictedCount++ % EvictionSamples] = Math.Max(evictedFilePages, 0);
        var samples = Math.Min(_evictedCount, EvictionSamples);
        double perSecond = 0;
        for (var i = 0; i < samples; i++)
            perSecond += _evicted[i];
        perSecond /= samples;
        var staleAfter = perSecond <= 0 || filePages <= 0 ? int.MaxValue : Math.Clamp(filePages / (2 * perSecond), 1, int.MaxValue);
        Volatile.Write(ref _staleAfterSeconds, (int)staleAfter);
    }

    internal void Tick(long freeBytes)
    {
        Interlocked.Increment(ref _clock);
        var warmingUp = _generation != 0;
        if (warmingUp && freeBytes < _endWarmUpBelowBytes)
        {
            _generation = 0;
        }
        else if (warmingUp == false && freeBytes > _startWarmUpAboveBytes)
        {
            _lastGeneration = (byte)(_lastGeneration % byte.MaxValue + 1); // skips 0: a segment never read ahead holds 0
            _generation = _lastGeneration;
        }
    }

    public void Dispose()
    {
        _timer?.Dispose();
        _evictions?.Dispose();
    }
}
