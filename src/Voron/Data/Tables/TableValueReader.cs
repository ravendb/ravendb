// -----------------------------------------------------------------------
//  <copyright file="a.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using Sparrow;
using Sparrow.Compression;

namespace Voron.Data.Tables
{
    public unsafe interface ITableValueReader
    {
        long Id { get; }
        
        int Count { get; }

        long ReadLong(int index);

        string ReadString(int index);

        string ReadStringWithPrefix(int index, int bytesToSkip);

        byte* Read(int index, out int size);
    }
    
    [StructLayout(LayoutKind.Sequential)]
    public readonly unsafe struct TableValueReader<TConstant> : ITableValueReader
        where TConstant : struct, INumericConstant
    {
        private readonly TableValueReader _reader;

        public long Id => _reader.Id;

        public int Count => _reader.Count;
        
        public long ReadLong(int index)
        {
            return _reader.ReadLong<TConstant>(index);
        }

        public string ReadString(int index)
        {
            return _reader.ReadString<TConstant>(index);
        }

        public string ReadStringWithPrefix(int index, int bytesToSkip)
        {
            return _reader.ReadStringWithPrefix<TConstant>(index, bytesToSkip);
        }

        public byte* Read(int index, out int size)
        {
            return _reader.Read<TConstant>(index, out size);
        }
    }

    [StructLayout(LayoutKind.Sequential)]
    public readonly unsafe struct TableValueReader : ITableValueReader
    {
        public long Id { get; }
        public readonly byte* Pointer;
        public readonly int Size;

        internal readonly int _offset;
        internal readonly int _elementSize;

        public TableValueReader(byte* ptr, int size) : this(-1, ptr, size)
        {}

        public TableValueReader(long id, byte* ptr, int size)
        {
            Id = id;
            Pointer = ptr;
            Size = size;

            _elementSize = size switch
            {
                > ushort.MaxValue => 4,
                > byte.MaxValue => 2,
                _ => 1
            };

            VariableSizeEncoding.Read<int>(ptr, out _offset);
        }

        public bool IsCompatible<TConstant>() where TConstant : struct, INumericConstant
        {
            return _elementSize == default(TConstant).N;
        }

        public static bool TryCast<TConstant>(ref TableValueReader reader, out TableValueReader<TConstant> output) 
            where TConstant : struct, INumericConstant
        {
            if (reader._elementSize == default(TConstant).N)
            {
                output = Unsafe.BitCast<TableValueReader, TableValueReader<TConstant>>(reader);
                return true;
            }

            Unsafe.SkipInit(out output);
            return false;
        }

        public int Count => VariableSizeEncoding.Read<int>(Pointer, out _);

        public long ReadLong(int index)
        {
            long l = *(long*)Read(index, out var size);
            Debug.Assert(sizeof(long) == size);
            return l;
        }

        internal long ReadLong<TConstant>(int index) where TConstant : struct, INumericConstant
        {
            long l = *(long*)Read<TConstant>(index, out var size);
            Debug.Assert(sizeof(long) == size);
            return l;
        }

        public string ReadString(int index)
        {
            byte* read = Read(index, out var size);
            return Encoding.UTF8.GetString(read, size);
        }

        internal string ReadString<TConstant>(int index) where TConstant : struct, INumericConstant
        {
            byte* read = Read<TConstant>(index, out var size);
            return Encoding.UTF8.GetString(read, size);
        }

        public string ReadStringWithPrefix(int index, int bytesToSkip)
        {
            byte* read = Read(index, out var size);
            return Encoding.UTF8.GetString(read + bytesToSkip, size - bytesToSkip);
        }

        public string ReadStringWithPrefix<TConstant>(int index, int bytesToSkip) where TConstant : struct, INumericConstant
        {
            byte* read = Read<TConstant>(index, out var size);
            return Encoding.UTF8.GetString(read + bytesToSkip, size - bytesToSkip);
        }

        // PERF: This table allow us to access the <mask, shift, indexAdjustment> tuple directly from a location in memory.
        // The reason why we use an extra integer is to avoid having to multiply by 3, by multiplying by 4 we can
        // multiply by applying shifts (the JIT will take care of that).
        private static ReadOnlySpan<int> ElementsTable => new int[] 
        {
            0, 0, 0, 0,                             // Index 0 (not used)
            0x000000FF, 24, 3, 0,                   // Index 1 when _elementSize == 1
            0x0000FFFF, 16, 2, 0,                   // Index 2 when _elementSize == 2
            0, 0, 0, 0,                             // Index 3 (not used)
            unchecked((int)0xFFFFFFFF), 0, 0, 0     // Index 4 when _elementSize == 4
        };

        internal byte* Read<TConstant>(int index, out int size) where TConstant : struct, INumericConstant
        {
            PortableExceptions.ThrowIfOnDebug<ArgumentOutOfRangeException>(index < 0 || index >= Count);

            // Calculate the starting pointer to the data, adjusted by the offset.
            var dataPtr = (Pointer + _offset);

            // Retrieve mask, shift, and index adjustment values from the table and bake all of them 
            // on into a constant. 
            uint mask = (uint)ElementsTable[4 * default(TConstant).N];
            int shift = ElementsTable[4 * default(TConstant).N + 1];
            int indexAdjustment = ElementsTable[4 * default(TConstant).N + 2];

            // Calculate the pointer to the exact byte position of the element, adjusted for indexing.
            // The key idea is that to avoid tripping on memory we don't own, indexing will consider the
            // element as the last memory address accessed. The index adjustment ensures this safety.
            byte* byteIndexPtr = dataPtr + index * default(TConstant).N - indexAdjustment;

            // Decode the position of the element within the data array using bit manipulation.
            int position = (int)((*(uint*)byteIndexPtr >> shift) & mask);

            // Determine if there is a subsequent element and calculate its position.
            // This helps to determine the boundary of the current element.
            var hasNext = index + 1 < Count;
            int nextPos = hasNext ? (int)((*(uint*)(byteIndexPtr + default(TConstant).N) >> shift) & mask) : (Size - _offset);

            size = nextPos - position;
            return dataPtr + position;
        }

        public byte* Read(int index, out int size)
        {
            return _elementSize switch
            {
                1 => Read<N1>(index, out size),
                2 => Read<N2>(index, out size),
                _ => Read<N4>(index, out size),
            };
        }
    }
}
