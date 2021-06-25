using System;
using System.Collections;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Sparrow;
using Sparrow.Collections;
using Sparrow.Json.Parsing;
using Sparrow.Server;
using Voron.Impl;
using Voron.Util;

namespace Voron.Data.Tables
{
    public unsafe class TableValueBuilder : IEnumerable
    {
        private readonly FastList<PtrSize> _values = new FastList<PtrSize>();
        private int _elementSize = 1;
        private bool _isDirty;
        private int _size;

        private readonly TableValueCompressor Compression;

        public TableValueBuilder()
        {
            Compression = new TableValueCompressor(this);
        }


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

                if (size + _values.Count + 1 > byte.MaxValue) _elementSize = 2;

                Return:
                _isDirty = false;
                return _elementSize;
            }
        }

        public int Size
        {
            get
            {
                return Compression.IsValid ? Compression.Size : RawSize;
            }
        }

        public int RawSize => _size + ElementSize * _values.Count + JsonParserState.VariableSizeIntSize(_values.Count);

        public int Count => _values.Count;

        IEnumerator IEnumerable.GetEnumerator()
        {
            throw new NotSupportedException("Only for the collection initializer syntax");
        }

        public void Reset()
        {
            _values.Clear();
            _size = 0;
            _elementSize = 1;
            _isDirty = false;
            Compression.Reset();
        }


        public int SizeOf(int index)
        {
            return _values[index].Size;
        }

        public ByteStringContext<ByteStringMemoryCache>.Scope SliceFromLocation(ByteStringContext context, int index, out Slice slice)
        {
            if (_values[index].IsValue)
            {
                ulong value = _values[index].Value;
                return Slice.From(context, (byte*)&value, _values[index].Size, out slice);
            }

            return Slice.External(context, _values[index].Ptr, _values[index].Size, out slice);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Add<T>(T value) where T : struct
        {
            PtrSize ptr = PtrSize.Create(value);
            Add(ref ptr);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Add(Slice buffer)
        {
            PtrSize ptr = PtrSize.Create(buffer.Content.Ptr, buffer.Content.Length);
            Add(ref ptr);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Add(Span<byte> buffer)
        {
            PtrSize ptr = PtrSize.Create(Unsafe.AsPointer(ref buffer[0]), buffer.Length);
            Add(ref ptr);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Add(void* pointer, int size)
        {
            PtrSize ptr = PtrSize.Create(pointer, size);
            Add(ref ptr);
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
            if (Compressed)
            {
                Compression.CopyTo(ptr);
                return;
            }

            JsonParserState.WriteVariableSizeInt(ref ptr, _values.Count);

            int elementSize = ElementSize;

            int pos = _values.Count * elementSize;
            byte* dataStart = ptr + pos;

            switch (elementSize)
            {
                case 1:
                    byte* bytePtr = ptr;
                    for (int i = 0; i < _values.Count; i++)
                    {
                        bytePtr[i] = (byte)pos;
                        pos += _values[i].Size;
                    }

                    break;
                case 2:
                    ushort* shortPtr = (ushort*)ptr;
                    for (int i = 0; i < _values.Count; i++)
                    {
                        shortPtr[i] = (ushort)pos;
                        pos += _values[i].Size;
                    }

                    break;
                case 4:
                    int* intPtr = (int*)ptr;
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

        public bool TryCompression(Table table, TableSchema schema)
        {
            var tx = table._tx;

            Compression.RawScope = tx.Allocator.Allocate(RawSize, out Compression.RawBuffer);
            CopyTo(Compression.RawBuffer.Ptr);

            return Compression.TryCompression(table, schema);
        }


        public bool TryCompression(Table table, TableSchema schema, ref byte* ptr, ref int size)
        {
            using var _ = table._tx.Allocator.FromPtr(ptr, size, ByteStringType.Immutable, out Compression.RawBuffer);
            var result = Compression.TryCompression(table, schema);

            if (result)
            {
                ptr = Compression.CompressedBuffer.Ptr;
                size = Compression.CompressedBuffer.Length;
            }
            return result;
        }

        public bool Compressed => Compression.Compressed;

        public bool CompressionTried => Compression.CompressionTried;

        public TableValueReader CreateReader(byte* pos)
        {
            if (Compression.IsValid)
                return Compression.CreateReader(pos);
            return new TableValueReader(pos, Size);
        }

    }
}
