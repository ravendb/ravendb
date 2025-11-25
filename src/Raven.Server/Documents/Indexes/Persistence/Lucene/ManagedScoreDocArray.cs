using System;
using System.Buffers;
using System.Collections.Generic;
using System.Numerics;
using System.Runtime.CompilerServices;

namespace Raven.Server.Documents.Indexes.Persistence.Lucene;

public class ManagedScoreDocArray : IDisposable
{
    private const int SingleItemSize = sizeof(int);
    private const int MaxItemsPerSegment = 64 * 1024 / SingleItemSize; // 16,384 items - 64KB limit to avoid LOH
    private const int InitialItems = 1 * 1024 / SingleItemSize; // 256 items - 1KB

    // math constants for the "Stable" phase
    // log2(16384) = 14. we can use bit shifting instead of division.
    private const int MaxItemsLog2 = 14;

    // sequence: 256 -> 512 -> 1024 -> 2048 -> 4096 -> 8192
    // the next one (16384) is the start of stable phase.
    // sum = 256 + 512 + ... + 8192 = 16128 items.
    private const int GrowthPhaseTotalItems = 16128;

    // there are 6 segments in the growth phase (0 to 5)
    // segment 6 is the first "Stable" (max Size) segment.
    private const int StablePhaseSegmentStartIndex = 6;

    // used to shift indexes for the Log2 calculation in the growth phase
    private const int GrowthPhaseShift = 8;

    private readonly List<Segment> _segments = new();

    // hot path caching
    private int[] _currentDocs;
    private float[] _currentScores;
    private int _currentSegmentUsed;
    private int _currentSegmentCapacity;

    private int _length;
    public int Length => _length;

    public void Add(int doc, float score)
    {
        if (_currentSegmentUsed == _currentSegmentCapacity)
        {
            EnsureCapacity();
        }

        _currentDocs[_currentSegmentUsed] = doc;
        _currentScores[_currentSegmentUsed] = score;

        _currentSegmentUsed++;
        _length++;
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private void EnsureCapacity()
    {
        if (_segments.Count > 0)
        {
            // if we have an active segment, update its 'Used' count before switching
            // we have to copy the value type back into the list because Segment is a struct,
            // and we modified the local field _currentSegmentUsed
            var lastSeg = _segments[^1];
            lastSeg.Used = _currentSegmentUsed;
            _segments[^1] = lastSeg;
        }

        int newSize;
        if (_segments.Count == 0)
        {
            newSize = InitialItems;
        }
        else
        {
            newSize = _currentSegmentCapacity == MaxItemsPerSegment
                ? MaxItemsPerSegment
                : Math.Min(_currentSegmentCapacity * 2, MaxItemsPerSegment);
        }

        var docs = ArrayPool<int>.Shared.Rent(newSize);
        var scores = ArrayPool<float>.Shared.Rent(newSize);

        var newSegment = new Segment
        {
            Docs = docs,
            Scores = scores,
            Used = 0
        };

        _segments.Add(newSegment);

        // update hot-path cache
        _currentDocs = docs;
        _currentScores = scores;
        _currentSegmentCapacity = newSize;
        _currentSegmentUsed = 0;
    }

    public (int Doc, float Score) this[int index]
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get
        {
            if ((uint)index >= (uint)_length)
                ThrowIndexOutOfRangeException();

            // stable region (index >= 16128)
            if (index >= GrowthPhaseTotalItems)
            {
                int relativeIndex = index - GrowthPhaseTotalItems;

                int segmentOffset = relativeIndex >> MaxItemsLog2;
                int indexInSegment = relativeIndex & (MaxItemsPerSegment - 1);

                // offset by the number of growth segments (6 segments: 256..8192)
                var seg = _segments[StablePhaseSegmentStartIndex + segmentOffset];
                return (seg.Docs[indexInSegment], seg.Scores[indexInSegment]);
            }

            // growth region
            return GetFromGrowthSegment(index);
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private (int Doc, float Score) GetFromGrowthSegment(int index)
    {
        int segIndex = BitOperations.Log2((uint)(index >> GrowthPhaseShift) + 1);
        int segmentStart = InitialItems * ((1 << segIndex) - 1);
        int indexInSegment = index - segmentStart;
        var seg = _segments[segIndex];
        return (seg.Docs[indexInSegment], seg.Scores[indexInSegment]);
    }

    public void Dispose()
    {
        foreach (var seg in _segments)
        {
            ArrayPool<int>.Shared.Return(seg.Docs);
            ArrayPool<float>.Shared.Return(seg.Scores);
        }

        _segments.Clear();
        _currentDocs = null;
        _currentScores = null;
        _length = _currentSegmentUsed = _currentSegmentCapacity = 0;
    }

    private static void ThrowIndexOutOfRangeException() => throw new IndexOutOfRangeException();

    private struct Segment
    {
        public int[] Docs;
        public float[] Scores;
        public int Used;
    }
}
