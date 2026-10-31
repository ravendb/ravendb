using System;
using System.Runtime.CompilerServices;
using Sparrow.Binary;
using Voron.Global;

namespace Voron.Impl.Paging;

/// <summary>
/// The per data file state of <see cref="PrefetchPolicy"/>: the warm-up in which each segment was last read ahead. It is
/// updated on every page access without synchronization: two threads may both read ahead the same segment, which is
/// harmless.
/// </summary>
internal sealed class SegmentPrefetchTable
{
    private readonly PrefetchPolicy _policy;
    private readonly int _segmentShift;
    private byte[] _segments;

    public readonly long SegmentSize;

    public SegmentPrefetchTable(PrefetchPolicy policy, long segmentSize, long fileSize)
    {
        _policy = policy;
        _segmentShift = Bits.MostSignificantBit(segmentSize);
        SegmentSize = 1L << _segmentShift;
        _segments = new byte[Math.Max(1, (fileSize >> _segmentShift) + 1)];
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool ShouldPrefetchSegment(long pageNumber, out long offset, out long bytes)
    {
        offset = 0;
        bytes = 0;
        var generation = _policy.Generation;
        if (generation == 0)
            return false; // memory is full

        var index = (pageNumber * Constants.Storage.PageSize) >> _segmentShift;
        var segments = _segments;
        if (index >= segments.Length)
            segments = Grow(index);
        if (segments[index] == generation)
            return false;

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
