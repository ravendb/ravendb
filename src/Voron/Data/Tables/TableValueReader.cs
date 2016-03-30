// -----------------------------------------------------------------------
//  <copyright file="a.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;

namespace Voron.Data.Tables
{

    public unsafe class TableValueReader
    {
        private readonly byte* _ptr;
        private readonly int _size;
        private readonly int _elementSize = 1;
        private readonly byte _count;

        public TableValueReader(byte* ptr, int size)
        {
            _ptr = ptr;
            _size = size;
            if (size > byte.MaxValue)
                _elementSize = 2;
            if (size > ushort.MaxValue)
                _elementSize = 4;
            _count = _ptr[0];
        }

        public long Id;

        public int Size => _size;

        public byte Count => _count;

        public byte* Read(int index, out int size)
        {
            if (index < 0 || index >= _count)
                throw new ArgumentOutOfRangeException(nameof(index));

            var pos = GetPositionByIndex(index);

            var nextPos = index + 1 < _count ? GetPositionByIndex(index + 1) : _size;
            size = nextPos - pos;
            return _ptr + pos;
        }

        private int GetPositionByIndex(int index)
        {
            switch (_elementSize)
            {
                case 1:
                    return _ptr[index + 1];
                case 2:
                    return ((ushort*)(_ptr + 1))[index];
                case 4:
                    return ((int*)(_ptr + 1))[index];
                default:
                    throw new ArgumentOutOfRangeException(nameof(_elementSize), "Unknown element size " + _elementSize);
            }
        }
    }
}