using System;
using System.Collections;
using System.Collections.Generic;
using Sparrow;
using Sparrow.Json.Parsing;
using Voron.Util;

namespace Voron.Data.Tables
{
    public unsafe class TableValueBuilder : IEnumerable
    {
        private readonly List<PtrSize> _values = new List<PtrSize>();
        private int _size;
        private int _elementSize = 1;

        public byte* Read(int index, out int size)
        {
            var ptrSize = _values[index];
            size = ptrSize.Size;
            return ptrSize.Ptr;
        }

        public void Add(Slice slice)
        {
            Add(slice.Content.Ptr, slice.Size);
        }

        public void Add(ulong* value)
        {
            Add((byte*)value, sizeof(ulong));
        }

        public void Add(long* value)
        {
            Add((byte*)value, sizeof(long));
        }

        public void Add(int* value)
        {
            Add((byte*)value, sizeof(int));
        }

        public void Add(uint* value)
        {
            Add((byte*)value, sizeof(uint));
        }

        public void Add(bool* value)
        {
            Add((byte*)value, sizeof(bool));
        }

        public void Add(byte* ptr, int size)
        {
            if (size < 0)
                throw new ArgumentException("Size cannot be negative", nameof(size));

            _values.Add(new PtrSize
            {
                Size = size,
                Ptr = ptr
            });
            _size += size;
            if (_size + _values.Count + 1 > byte.MaxValue)
            {
                _elementSize = 2;
            }
            if (_size + _values.Count * 2 + 1 > ushort.MaxValue)
            {
                _elementSize = 4;
            }
        }

        public int Size => _size + _elementSize * _values.Count + JsonParserState.VariableSizeIntSize(_values.Count);

        public void CopyTo(byte* ptr)
        {
            JsonParserState.WriteVariableSizeInt(ref ptr, _values.Count);
            var pos = _values.Count * _elementSize;
            var dataStart = ptr + pos;
            switch (_elementSize)
            {
                case 1:
                    var bytePtr = ptr;
                    for (int i = 0; i < _values.Count; i++)
                    {
                        bytePtr[i] = (byte)pos;
                        pos += _values[i].Size;
                    }
                    break;
                case 2:
                    var shortPtr = (ushort*)ptr;
                    for (int i = 0; i < _values.Count; i++)
                    {
                        shortPtr[i] = (ushort)pos;
                        pos += _values[i].Size;
                    }
                    break;
                case 4:
                    var intPtr = (int*)ptr;
                    for (int i = 0; i < _values.Count; i++)
                    {
                        intPtr[i] = pos;
                        pos += _values[i].Size;
                    }
                    break;
                default:
                    throw new ArgumentOutOfRangeException(nameof(_elementSize), "Unknown element size " + _elementSize);
            }
            for (int i = 0; i < _values.Count; i++)
            {
                Memory.Copy(dataStart, _values[i].Ptr, _values[i].Size);
                dataStart += _values[i].Size;
            }
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            throw new NotSupportedException("Only for the collection initializer syntax");
        }
    }
}