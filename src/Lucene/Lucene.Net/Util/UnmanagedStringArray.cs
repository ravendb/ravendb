using System;
using System.Buffers;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using System.Text;
using Lucene.Net.Index;

namespace Lucene.Net.Util
{
    public unsafe class UnmanagedStringArray : IDisposable
    {
        public class Segment : IDisposable
        {
            public readonly int Size;

            public byte* Start;
            public byte* CurrentPosition => Start + Used;
            public int Free => Size - Used;
            public int Used;

            public delegate byte* AllocateSegmentDelegate(long size);
            public delegate void FreeSegmentDelegate(byte* ptr, long size);

            public static AllocateSegmentDelegate AllocateMemory = (size) => (byte*) Marshal.AllocHGlobal((IntPtr) size);
            public static FreeSegmentDelegate FreeMemory = (ptr, _) => Marshal.FreeHGlobal((IntPtr) ptr);

            public Segment(int size)
            {
                Start = AllocateMemory(size);
                Used = 0;
                Size = size;
            }

            public void Add(ushort size, out byte* position)
            {
                position = CurrentPosition;
                *(ushort*) CurrentPosition = size;

                Used += sizeof(short) + size;
                if (Used > Size)
                    ThrowOutOfRange(size);
            }

            private void ThrowOutOfRange(ushort size)
            {
                throw new ArgumentOutOfRangeException(nameof(Used),$"Requested:{size}, Used: {Used}, but my max size is {Size}");
            }

            public void Dispose()
            {
                GC.SuppressFinalize(this);
                if (Start != null)
                {
                    FreeMemory(Start, Size);
                }
                Start = null;
            }

            ~Segment()
            {
                Dispose();
            }
        }

        public struct UnmanagedString : IComparable
        {
            public byte* Start;

            public int Size => IsNull ? 0 : *(ushort*) Start;
            public Span<byte> StringAsBytes => new Span<byte>(Start + sizeof(ushort), Size);
            public bool IsNull => Start == default;
            
            public override string ToString()
            {
                return Encoding.UTF8.GetString(StringAsBytes);
            }

            public static int CompareOrdinal(UnmanagedString strA, UnmanagedString strB)
            {
                if (strA.IsNull && strB.IsNull)
                    return 0;

                if (strB.IsNull)
                    return 1;

                if (strA.IsNull)
                    return -1;

                return strA.StringAsBytes.SequenceCompareTo(strB.StringAsBytes);
            }

            public static int CompareOrdinal(UnmanagedString strA, Span<byte> strB)
            {
                if (strA.IsNull && strB == null)
                    return 0;

                if (strB == null)
                    return 1;

                if (strA.IsNull)
                    return -1;

                return strA.StringAsBytes.SequenceCompareTo(strB);
            }

            public static int CompareOrdinal(Span<byte> strA, UnmanagedString strB)
            {
                return -CompareOrdinal(strB, strA);
            }

            public int CompareTo(object other)
            {
                if (other == null)
                    return CompareOrdinal(this, null);

                if (other is UnmanagedString us)
                    return CompareOrdinal(this, us);

                if (other is string s)
                {
                    byte[] arr = null;
                    Span<byte> stringAsBytes = stackalloc byte[0]; // relax the compiler
                    var stringAsSpan = s.AsSpan();

                    var size = (ushort) Encoding.UTF8.GetByteCount(stringAsSpan);

                    if (size <= 256) // allocate on the stack
                    {
                        stringAsBytes = stackalloc byte[size];
                    }
                    else
                    {
                        var pooledSize = BitUtil.NextHighestPowerOfTwo(size);
                        arr = ArrayPool<byte>.Shared.Rent(pooledSize);
                        stringAsBytes = new Span<byte>(arr, 0, size);
                    }

                    try
                    {
                        Encoding.UTF8.GetBytes(stringAsSpan, stringAsBytes);
                        return CompareOrdinal(this, stringAsBytes);
                    }
                    finally
                    {
                        if (arr != null)
                            ArrayPool<byte>.Shared.Return(arr);
                    }
                }

                throw new ArgumentException($"Unknown type {other.GetType()} for comparison");
            }
        }

        private UnmanagedString[] _strings;
        private List<Segment> _segments = new List<Segment>();

        public int Length => _index;
        public int _index = 1;

        public UnmanagedStringArray(int size)
        {
            _strings = new UnmanagedString[size];
        }

        private Segment GetSegment(int size)
        {
            if (_segments.Count == 0)
            {
                var firstSegmentSize = AdjustSegmentSize(4096, size);
                return GetAndAddNewSegment(firstSegmentSize);
            }

            // naive but simple
            var seg = _segments[^1];
            if (seg.Free > size)
                return seg;

            var segmentSize = Math.Min(1024 * 1024, seg.Size * 2);
            segmentSize = AdjustSegmentSize(segmentSize, size);

            return GetAndAddNewSegment(segmentSize);
        }

        private static int AdjustSegmentSize(int segmentSize, int size)
        {
            if (size > segmentSize)
            {
                // too big, make it 4KB aligned so if there is wasted space in the end, it is just
                // a single memory page. In most cases, if we have one big size, we'll have a lot, so we
                // want to avoid power of two here
                segmentSize = ((size / 4096) + (size % 4096 == 0 ? 0 : 1)) * 4096;
            }

            return segmentSize;
        }

        private Segment GetAndAddNewSegment(int segmentSize)
        {
            var newSegment = new Segment(segmentSize);
            _segments.Add(newSegment);
            return newSegment;
        }

        public void Add(Span<char> str)
        {
            var size = (ushort) Encoding.UTF8.GetByteCount(str);
            var segment = GetSegment(size + sizeof(ushort));

            segment.Add(size, out var position);

            Encoding.UTF8.GetBytes(str, new Span<byte>(position + sizeof(ushort), size));

            _strings[_index].Start = position;
            _index++;
        }

        public void AddDeleted(TermBuffer termBuffer)
        {
            // since we are doing a binary search, we must keep the order of the terms

            if (_index == 1)
            {
                // we must allocate the first one
                Add(termBuffer.TextAsSpan);
                return;
            }

            _strings[_index].Start = _strings[_index - 1].Start;
            _index++;
        }

        public UnmanagedString this[int position]
        {
            get => _strings[position];
            set => _strings[position] = value;
        }

        public void Dispose()
        {
            foreach (var segment in _segments)
            {
                segment.Dispose();
            }

            _segments.Clear();
        }
    }
}
