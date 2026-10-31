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
    private readonly Timer _timer;

    public PrefetchPolicy(long totalMemoryBytes, bool startTimer)
    {
        _endWarmUpBelowBytes = totalMemoryBytes / 100 * StopPercent;
        _startWarmUpAboveBytes = totalMemoryBytes / 100 * StartPercent;
        if (startTimer == false)
            return;

        // free memory reads as zero until the server's low-memory monitor runs: no warm-up without it
        _timer = new Timer(_ => Tick(LowMemoryNotification.Instance.LastMemoryInfo.AvailableMemory.GetValue(SizeUnit.Bytes)),
            null, 0, TickMs);
    }

    /// <summary>1 to 255 during a warm-up, 0 when memory is full.</summary>
    public byte Generation => _generation;

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

    public void Dispose() => _timer?.Dispose();
}
