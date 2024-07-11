// -----------------------------------------------------------------------
//  <copyright file="a.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Text;
using Sparrow;
using Sparrow.Binary;
using Sparrow.Compression;
using Voron.Util.Conversion;

namespace Voron.Data.Tables
{
    public readonly unsafe struct TableValueReader
    {
        public readonly long Id;
        public readonly byte* Pointer;
        public readonly int Size;
        private readonly int _offset;

        private readonly int _elementSize;

        public TableValueReader(byte* ptr, int size) : this(-1, ptr, size)
        {
            
        }

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

        public int Count => VariableSizeEncoding.Read<int>(Pointer, out _);

        public long ReadLong(int index)
        {
            long l = *(long*)Read(index, out var size);
            Debug.Assert(sizeof(long) == size);
            return l;
        }

        public string ReadString(int index)
        {
            byte* read = Read(index, out var size);
            return Encoding.UTF8.GetString(read, size);
        }

        public string ReadStringWithPrefix(int index, int bytesToSkip)
        {
            byte* read = Read(index, out var size);
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
        
        public byte* Read(int index, out int size)
        {
            PortableExceptions.ThrowIfOnDebug<ArgumentOutOfRangeException>(index < 0 || index >= Count);

            // Calculate the starting pointer to the data, adjusted by the offset.
            var dataPtr = (Pointer + _offset);

            // Retrieve mask, shift, and index adjustment values from the table.
            int elementsLoc = 4 * _elementSize;
            uint mask = (uint)ElementsTable[elementsLoc];
            int shift = ElementsTable[elementsLoc + 1];
            int indexAdjustment = ElementsTable[elementsLoc + 2];

            // Calculate the pointer to the exact byte position of the element, adjusted for indexing.
            // The key idea is that to avoid tripping on memory we don't own, indexing will consider the
            // element as the last memory address accessed. The index adjustment ensures this safety.
            byte* byteIndexPtr = dataPtr + index * _elementSize - indexAdjustment;

            // Decode the position of the element within the data array using bit manipulation.
            int position = (int)((*(uint*)byteIndexPtr >> shift) & mask);

            // Determine if there is a subsequent element and calculate its position.
            // This helps to determine the boundary of the current element.
            var hasNext = index + 1 < Count;
            int nextPos = hasNext ? (int)((*(uint*)(byteIndexPtr + _elementSize) >> shift) & mask) : (Size - _offset);

            size = nextPos - position;
            return dataPtr + position;
        }

        [DoesNotReturn]
        private void ThrowInvalidElementSize()
        {
            throw new ArgumentOutOfRangeException(nameof(_elementSize), "Unknown element size " + _elementSize);
        }

    }
}
