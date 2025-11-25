using System.Diagnostics;
using System.Runtime.InteropServices;

namespace Raven.Server.Documents.Indexes.Persistence.Lucene;

using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Numerics;

public unsafe class UnmanagedScoreDocArray : IDisposable
{
    private const int MinSegmentSize = 4096;
    private const int MaxSegmentSize = 1 * 1024 * 1024; // 1MB
    private static readonly int ElementSize = sizeof(ScoreDocStruct); // 8 bytes
    private static readonly int ItemsPerMaxSegment = MaxSegmentSize / ElementSize; // 1MB / 8 bytes = 131,072 items per max segment

    private static readonly int GrowthPhaseSegmentCount;
    private static readonly int GrowthPhaseItemCount;

    private readonly List<FixedSegment> _segments = new();
    private FixedSegment _currentSegment; // cache current segment to avoid List lookup on every Add
    private int _length;

    public int Length => _length;

    static UnmanagedScoreDocArray()
    {
        int currentSize = MinSegmentSize;
        var starts = new List<int>();
        int totalItems = 0;

        int segCount = 0;

        // simulate the growth until we hit MaxSegmentSize
        while (currentSize < MaxSegmentSize)
        {
            starts.Add(totalItems);

            int itemsInSeg = currentSize / ElementSize;
            totalItems += itemsInSeg;

            currentSize *= 2;
            segCount++;
        }

        GrowthPhaseSegmentCount = segCount; // should be 8
        GrowthPhaseItemCount = totalItems;  // should be 130,560

        Debug.Assert(GrowthPhaseSegmentCount == 8);
        Debug.Assert(GrowthPhaseItemCount == 130_560);
    }

    public void Add(int doc, float score)
    {
        if (_currentSegment == null || _currentSegment.Free < ElementSize)
        {
            EnsureCapacity();
        }

        Debug.Assert(_currentSegment != null, nameof(_currentSegment) + " != null");

        var p = (ScoreDocStruct*)_currentSegment.Allocate(sizeof(ScoreDocStruct));
        p->Doc = doc;
        p->Score = score;

        _length++;
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private void EnsureCapacity()
    {
        int newSize;

        if (_segments.Count == 0)
        {
            newSize = MinSegmentSize;
        }
        else
        {
            newSize = _currentSegment.Size == MaxSegmentSize
                ? MaxSegmentSize
                : Math.Min(_currentSegment.Size * 2, MaxSegmentSize);
        }

        var seg = new FixedSegment(newSize);
        _segments.Add(seg);
        _currentSegment = seg;
    }

    public ScoreDocStruct this[int index]
    {
        get
        {
            if (index < 0 || index >= _length)
                ThrowIndexOutOfRangeException();

            if (index >= GrowthPhaseItemCount)
            {
                int relativeIndex = index - GrowthPhaseItemCount;

                int segmentOffset = relativeIndex / ItemsPerMaxSegment;
                int indexInSegment = relativeIndex % ItemsPerMaxSegment;

                // We skip the growth segments in the list
                var seg = _segments[GrowthPhaseSegmentCount + segmentOffset];
                return *(ScoreDocStruct*)(seg.Start + (indexInSegment * ElementSize));
            }

            return GetFromGrowthSegment(index);
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private ScoreDocStruct GetFromGrowthSegment(int index)
    {
        // 1. Find which segment index (k) we are in using Log2.
        // Shift right by 9 is equivalent to dividing by 512.
        // We verify: 
        // if index = 0    -> Log2(0 + 1) = 0. Correct.
        // if index = 512  -> Log2(1 + 1) = 1. Correct.
        // if index = 1536 -> Log2(3 + 1) = 2. Correct.
        int segIndex = BitOperations.Log2((uint)(index >> 9) + 1);

        // 2. Calculate where that segment starts.
        // Formula: 512 * (2^k - 1)
        // (1 << segIndex) is 2^k
        int segmentStart = 512 * ((1 << segIndex) - 1);

        int offsetInSeg = index - segmentStart;

        var seg = _segments[segIndex];
        return *(ScoreDocStruct*)(seg.Start + (offsetInSeg * ElementSize));
    }

    private static void ThrowIndexOutOfRangeException()
    {
        throw new IndexOutOfRangeException();
    }

    public void Dispose()
    {
        Console.WriteLine($"dispose");
        foreach (var seg in _segments)
            seg.Dispose();

        _segments.Clear();
        _currentSegment = null;
        _length = 0;
    }

    [StructLayout(LayoutKind.Sequential)]
    public struct ScoreDocStruct
    {
        public int Doc;
        public float Score;
    }

    private class FixedSegment : IDisposable
    {
        public readonly int Size;

        public byte* Start;
        private byte* CurrentPosition => Start + Used;
        public int Free => Size - Used;
        public int Used;

        public FixedSegment(int size)
        {
            Start = Sparrow.Utils.NativeMemory.AllocateMemory(size);
            Used = 0;
            Size = size;
        }

        public byte* Allocate(int size)
        {
            var position = CurrentPosition;

            Used += size;
            if (Used > Size)
                ThrowOutOfRange(size);

            return position;
        }

        private void ThrowOutOfRange(int size)
        {
            throw new ArgumentOutOfRangeException(nameof(Used), $"Allocate operation failed: Requested size {size}, Used: {Used}, Max Size: {Size}");
        }

        public void Dispose()
        {
            GC.SuppressFinalize(this);
            if (Start != null)
            {
                Sparrow.Utils.NativeMemory.Free(Start, Size);
            }
            Start = null;
        }

        ~FixedSegment()
        {
            Dispose();
        }
    }
}
