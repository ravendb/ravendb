using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
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
        public int Size => _size + _elementSize * _values.Count + JsonParserState.VariableSizeIntSize(_values.Count);

        public ByteStringContext.Scope SliceFromLocation(ByteStringContext context, int index, out Slice slice)
        {
            if (_values[index].IsValue)
            {
                ulong value = _values[index].Value;
                return Slice.From(context, (byte*) &value, _values[index].Size, out slice);
            }

            return Slice.External(context, _values[index].Ptr, _values[index].Size, out slice);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Add<T>(T value) where T : struct
        {
            Add(PtrSize.Create(value));
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Add(void* ptr, int size)
        {
            Add(PtrSize.Create(ptr, size));
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        IEnumerator IEnumerable.GetEnumerator()
        {
            throw new NotSupportedException("Only for the collection initializer syntax");
        }

        private void Add(PtrSize ptr)
        {
            if (ptr.Size < 0)
                throw new ArgumentException("Size cannot be negative", nameof(ptr.Size));

            _values.Add(ptr);
            _size += ptr.Size;

            if (_size + _values.Count + 1 > byte.MaxValue)
            {
                _elementSize = 2;
            }

            if (_size + _values.Count * 2 + 1 > ushort.MaxValue)
            {
                _elementSize = 4;
            }
        }

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

            ulong value; // Do not move inside because we require value to exist inside the for loop. 
            for (int i = 0; i < _values.Count; i++)
            {
                PtrSize p = _values[i];

                byte* srcPtr;
                if (_values[i].IsValue)
                {
                    value = p.Value;
                    srcPtr = (byte*)&value; // This generates an alias on value
                }
                else
                {
                    srcPtr = p.Ptr;                    
                }

                Memory.CopyInline(dataStart, srcPtr, p.Size);

                dataStart += p.Size;
                value = 0; // This ensures there cannot be any JIT optimization that could reuse the memory location.          
            }
        }
    }
}