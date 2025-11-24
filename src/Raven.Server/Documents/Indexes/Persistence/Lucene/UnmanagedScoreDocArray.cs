using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;

namespace Raven.Server.Documents.Indexes.Persistence.Lucene;

public unsafe class UnmanagedScoreDocArray : IDisposable
{
    private readonly List<FixedSegment> _segments = new();
    private readonly List<int> _segmentCounts = new();
    private int _length;

    private const int MaxSegmentSize = 1 * 1024 * 1024; // 1MB

    public int Length => _length;

    public void Add(int doc, float score)
    {
        var segment = GetSegment();

        var p = (ScoreDocStruct*)segment.Allocate(sizeof(ScoreDocStruct));
        p->Doc = doc;
        p->Score = score;

        _segmentCounts[^1]++;

        _length++;
    }

    private FixedSegment GetSegment()
    {
        if (_segments.Count == 0)
        {
            return AddSegment(4096);
        }

        var seg = _segments[^1];
        if (seg.Free >= sizeof(ScoreDocStruct))
            return seg;

        // grow segment size but cap at 1MB
        int newSize = Math.Min(seg.Size * 2, MaxSegmentSize);
        return AddSegment(newSize);
    }

    private FixedSegment AddSegment(int size)
    {
        var seg = new FixedSegment(size);
        _segments.Add(seg);

        _segmentCounts.Add(0);

        return seg;
    }

    public ScoreDocStruct this[int index]
    {
        get
        {
            if (index < 0 || index >= _length)
                ThrowIndexOutOfRangeException();

            //TODO: can be optimized because we know the segment sizes

            int remaining = index;

            for (int i = 0; i < _segments.Count; i++)
            {
                int count = _segmentCounts[i];
                if (remaining < count)
                {
                    int offset = remaining * sizeof(ScoreDocStruct);
                    return *(ScoreDocStruct*)(_segments[i].Start + offset);
                }

                remaining -= count;
            }

            ThrowUnexpectedException();
            return default;
        }
    }

    private static void ThrowUnexpectedException()
    {
        throw new Exception("Index calculation error");
    }

    private static void ThrowIndexOutOfRangeException()
    {
        throw new IndexOutOfRangeException();
    }

    public void Dispose()
    {
        foreach (var seg in _segments)
            seg.Dispose();

        _segments.Clear();
        _segmentCounts.Clear();
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
