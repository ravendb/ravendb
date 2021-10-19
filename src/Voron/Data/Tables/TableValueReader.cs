// -----------------------------------------------------------------------
//  <copyright file="a.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Diagnostics;
using System.Text;
using Sparrow.Json;

namespace Voron.Data.Tables
{
    public unsafe struct TableValueReader
    {
        private readonly byte* _dataPtr;
        private readonly int _dataSize;
        private readonly int _elementSize;

        public readonly long Id;

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

            Count = BlittableJsonReaderBase.ReadVariableSizeInt(ptr, 0, out byte offset);
            _dataPtr = Pointer + offset;
            _dataSize = Size - offset;
        }

        public int Size { get; }

        public int Count { get; }

        public byte* Pointer { get; }

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

            switch (_elementSize)
            {
                case 1:
                    position = _dataPtr[index];
                    nextPos = hasNext ? _dataPtr[index + 1] : _dataSize;
                    break;
                case 2:
                    position = ((ushort*) _dataPtr)[index];
                    nextPos = hasNext ? ((ushort*) _dataPtr)[index + 1] : _dataSize;
                    break;
                case 4:
                    position = ((int*) _dataPtr)[index];
                    nextPos = hasNext ? ((int*) _dataPtr)[index + 1] : _dataSize;
                    break;
                default:
                    ThrowInvalidElementSize();
                    goto case 1; // never hit
            }

            size = nextPos - position;
            return _dataPtr + position;
        }

        private void ThrowInvalidElementSize()
        {
            throw new ArgumentOutOfRangeException(nameof(_elementSize), "Unknown element size " + _elementSize);
        }

        private static void ThrowIndexOutOfRange()
        {
            // ReSharper disable once NotResolvedInText
            throw new ArgumentOutOfRangeException("index");
        }
    }
}
