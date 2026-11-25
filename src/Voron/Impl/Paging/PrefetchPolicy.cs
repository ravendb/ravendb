using System;
using System.Threading;
using Sparrow;
using Sparrow.LowMemory;
using Sparrow.Server.LowMemory;

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
    private long _epoch;
    private long _evictedInEpoch;
    private const int TicksPerEpochWithoutEvictions = 8;
    private int _ticksWithoutEvictions;
    private readonly PageCacheEvictions _evictions;
    private readonly Timer _timer;

    public PrefetchPolicy(long totalMemoryBytes, bool startTimer)
    {
        _endWarmUpBelowBytes = totalMemoryBytes / 100 * StopPercent;
        _startWarmUpAboveBytes = totalMemoryBytes / 100 * StartPercent;
        if (startTimer == false)
            return;

        _evictions = PageCacheEvictions.TryCreate();
        // free memory reads as zero until the server's low-memory monitor runs: no warm-up without it
        _timer = new Timer(_ =>
        {
            Tick(LowMemoryNotification.Instance.LastMemoryInfo.AvailableMemory.GetValue(SizeUnit.Bytes));
            if (_evictions == null)
            {
                if (++_ticksWithoutEvictions % TicksPerEpochWithoutEvictions == 0)
                    Interlocked.Increment(ref _epoch); // Windows and macOS report no evictions
            }
            else if (_evictions.TryRead(out var evicted, out var filePages))
            {
                AddEvictions(evicted, filePages);
            }
        }, null, 0, TickMs);
    }

    /// <summary>1 to 255 during a warm-up, 0 when memory is full.</summary>
    public byte Generation => _generation;

    // Page hints age by epochs: an epoch ends each time the kernel evicts 1/16 of the file pages in the page cache. A hint is
    // stale after 8 epochs (half the cache replaced), so a page that may have left the cache is hinted again, however the
    // evictions are spread in time. Without eviction counts (Windows and macOS) an epoch ends every 8 seconds.
    public const int EpochsPerCache = 16;
    public const int StaleAfterEpochs = EpochsPerCache / 2;

    public long PageCacheEpoch => Volatile.Read(ref _epoch);

    internal void AddEvictions(long evictedFilePages, long filePages)
    {
        if (filePages <= 0)
            return;
        var pagesPerEpoch = Math.Max(filePages / EpochsPerCache, 1);
        var evicted = _evictedInEpoch + Math.Max(evictedFilePages, 0);
        _evictedInEpoch = evicted % pagesPerEpoch;
        if (evicted >= pagesPerEpoch)
            Interlocked.Add(ref _epoch, evicted / pagesPerEpoch);
    }

    internal void Tick(long freeBytes)
    {
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
