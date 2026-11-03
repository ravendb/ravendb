using System;
using System.Runtime.CompilerServices;
using Sparrow.Binary;
using Voron.Global;
using Voron.Impl.Journal;

namespace Voron.Impl.Paging;

/// <summary>
/// Per data file state of <see cref="PrefetchPolicy"/>: the warm-up in which each segment was last read ahead on a first
/// touch. Updated without synchronization: a segment read ahead twice is harmless.
/// </summary>
internal sealed class SegmentPrefetchTable
{
    private readonly PrefetchPolicy _policy;
    private readonly DeviceWriteBudget _deviceBudget;
    private readonly int _segmentShift;
    private byte[] _segments;

    public readonly long SegmentSize;

    public SegmentPrefetchTable(PrefetchPolicy policy, DeviceWriteBudget deviceBudget, long segmentSize, long fileSize)
    {
        _policy = policy;
        _deviceBudget = deviceBudget;
        _segmentShift = Bits.MostSignificantBit(segmentSize);
        SegmentSize = 1L << _segmentShift;
        _segments = new byte[Math.Max(1, (fileSize >> _segmentShift) + 1)];
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    /// <param name="numberOfAllocatedPages">the file's current size: a hint may carry -1 or a stale page number</param>
    public bool ShouldPrefetchSegment(long pageNumber, long numberOfAllocatedPages, out long offset, out long bytes)
    {
        offset = 0;
        bytes = 0;
        var generation = _policy.Generation;
        if (generation == 0)
            return false; // memory is full
        if ((ulong)pageNumber >= (ulong)numberOfAllocatedPages)
            return false; // not a page of the file

        var index = (pageNumber * Constants.Storage.PageSize) >> _segmentShift;
        var segments = _segments;
        if (index >= segments.Length)
            segments = Grow(index);
        if (segments[index] == generation)
            return false;

        if (_deviceBudget?.TryStartReadAhead(SegmentSize) == false)
            return false; // the device is busy: a later touch reads the segment ahead

        segments[index] = generation;
        offset = index << _segmentShift;
        bytes = SegmentSize;
        return true;
    }

    private byte[] Grow(long index)
    {
        var current = _segments;
        if (index < current.Length)
            return current;
        var grown = new byte[Math.Max(index + 1, current.Length * 2)];
        Array.Copy(current, grown, current.Length);
        _segments = grown; // a racing reader may mark a segment in the old array, and read it ahead once more later
        return grown;
    }
}
