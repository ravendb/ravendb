// -----------------------------------------------------------------------
//  <copyright file="a.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using Sparrow.Json;

namespace Voron.Data.Tables
{
    public unsafe class TableValueReader
    {
        private readonly byte* _dataPtr;
        private readonly int _dataSize;
        private readonly int _elementSize = 1;

        public long Id;

        public TableValueReader(byte* ptr, int size)
        {
            Pointer = ptr;
            Size = size;
            if (size > byte.MaxValue)
                _elementSize = 2;
            if (size > ushort.MaxValue)
                _elementSize = 4;

            byte offset;
            Count = BlittableJsonReaderBase.ReadVariableSizeInt(ptr, 0, out offset);
            _dataPtr = Pointer + offset;
            _dataSize = Size - offset;
        }

        public int Size { get; }

        public int Count { get; }

        public byte* Pointer { get; }

        public byte* Read(int index, out int size)
        {
            var hasNext = index + 1 < Count;

            if ((index < 0) || (index >= Count))
                throw new ArgumentOutOfRangeException(nameof(index));

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
                    throw new ArgumentOutOfRangeException(nameof(_elementSize), "Unknown element size " + _elementSize);
            }

            size = nextPos - position;
            return _dataPtr + position;
        }
    }
}