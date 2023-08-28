// -----------------------------------------------------------------------
//  <copyright file="a.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Text;
using Sparrow.Compression;

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

            if (size > ushort.MaxValue)
                _elementSize = 4;
            else if (size > byte.MaxValue)
                _elementSize = 2;
            else
                _elementSize = 1;

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

        public byte* Read(int index, out int size)
        {
            var hasNext = index + 1 < Count;

            if ((index < 0) || (index >= Count))
                ThrowIndexOutOfRange();

            int position;
            int nextPos;

            var dataPtr = (Pointer + _offset);
            switch (_elementSize)
            {
                case 1:
                    position = dataPtr[index];
                    nextPos = hasNext ? dataPtr[index + 1] : (Size - _offset);
                    break;
                case 2:
                    position = ((ushort*)dataPtr)[index];
                    nextPos = hasNext ? ((ushort*)dataPtr)[index + 1] : (Size - _offset);
                    break;
                case 4:
                    position = ((int*)dataPtr)[index];
                    nextPos = hasNext ? ((int*)dataPtr)[index + 1] : (Size - _offset);
                    break;
                default:
                    ThrowInvalidElementSize();
                    goto case 1; // never hit
            }

            size = nextPos - position;
            return dataPtr + position;
        }

        [DoesNotReturn]
        private void ThrowInvalidElementSize()
        {
            throw new ArgumentOutOfRangeException(nameof(_elementSize), "Unknown element size " + _elementSize);
        }

        [DoesNotReturn]
        private static void ThrowIndexOutOfRange()
        {
            // ReSharper disable once NotResolvedInText
            throw new ArgumentOutOfRangeException("index");
        }
    }
}
