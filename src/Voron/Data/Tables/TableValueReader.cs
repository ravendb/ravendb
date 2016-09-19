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

        public byte* Pointer => _ptr;

        public byte* Read(int index, out int size)
        {
            byte* ptr = _ptr + 1;
            bool hasNext = index + 1 < _count;

            if (index < 0 || index >= _count)
                throw new ArgumentOutOfRangeException(nameof(index));

            int position;
            int nextPos;                        

            switch ( _elementSize )
            {
                case 1:
                    position = ptr[index];
                    nextPos = hasNext ? ptr[index + 1] : _size;
                    break;
                case 2:
                    position = ((ushort*)ptr)[index];
                    nextPos = hasNext ? ((ushort*)ptr)[index + 1] : _size;
                    break;
                case 4:
                    position = ((int*)ptr)[index];
                    nextPos = hasNext ? ((int*)ptr)[index + 1] : _size;
                    break;
                default:
                    throw new ArgumentOutOfRangeException(nameof(_elementSize), "Unknown element size " + _elementSize);
            }

            size = nextPos - position;
            return _ptr + position;
        }

        public long* Read(int index, out object size)
        {
            throw new NotImplementedException();
        }
    }
}