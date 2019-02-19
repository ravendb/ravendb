using System;
using System.Collections;
using System.Runtime.CompilerServices;
using Sparrow;
using Sparrow.Collections;
using Sparrow.Json.Parsing;
using Sparrow.Server;
using Voron.Util;

namespace Voron.Data.Tables
{
    public unsafe class TableValueBuilder : IEnumerable
    {
        public void Reset()
        {
           _values.Clear();
           _size = 0;
           _elementSize = 1;
           _isDirty = false;
        }

        private readonly FastList<PtrSize> _values = new FastList<PtrSize>();

        private int _size;
        private bool _isDirty;
        private int _elementSize = 1;

        public int ElementSize
        {
            get
            {
                if (!_isDirty)
                    return _elementSize;

                int size = _size;
                if (size + _values.Count * 2 + 1 > ushort.MaxValue)
                {
                    _elementSize = 4;
                    goto Return;
                }

                if (size + _values.Count + 1 > byte.MaxValue)
                {
                    _elementSize = 2;
                }

                Return:
                _isDirty = false;
                return _elementSize;
            }
        }

        public int Size => _size + ElementSize * _values.Count + JsonParserState.VariableSizeIntSize(_values.Count);

        public int Count => _values.Count;

        public int SizeOf(int index)
        {
            return _values[index].Size;
        }

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
            var ptr = PtrSize.Create(value);
            Add(ref ptr);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Add(Slice buffer)
        {
            var ptr = PtrSize.Create(buffer.Content.Ptr, buffer.Content.Length);
            Add(ref ptr);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Add(void* pointer, int size)
        {
            var ptr = PtrSize.Create(pointer, size);
            Add(ref ptr);
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            throw new NotSupportedException("Only for the collection initializer syntax");
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void Add(ref PtrSize ptr)
        {
#if DEBUG
            if (ptr.Size < 0)
                ThrowSizeCannotBeNegative(nameof(ptr.Size));
#endif

            _values.Add(ptr);
            _size += ptr.Size;
            _isDirty = true;
        }

        private void ThrowSizeCannotBeNegative(string argument)
        {
            throw new ArgumentException("Size cannot be negative", argument);
        }

        public void CopyTo(byte* ptr)
        {
            JsonParserState.WriteVariableSizeInt(ref ptr, _values.Count);

            int elementSize = ElementSize;

            var pos = _values.Count * elementSize;
            var dataStart = ptr + pos;

            switch (elementSize)
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
                    throw new ArgumentOutOfRangeException(nameof(ElementSize), "Unknown element size " + ElementSize);
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

                Memory.Copy(dataStart, srcPtr, p.Size);

                dataStart += p.Size;
                value = 0; // This ensures there cannot be any JIT optimization that could reuse the memory location.          
            }
        }
    }
}