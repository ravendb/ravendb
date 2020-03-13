using System;
using System.Collections;
using System.Runtime.CompilerServices;
using Sparrow;
using Sparrow.Collections;
using Sparrow.Json.Parsing;
using Sparrow.Server;
using Voron.Data.RawData;
using Voron.Impl;
using Voron.Util;

namespace Voron.Data.Tables
{
    public unsafe class TableValueBuilder : IEnumerable
    {
        private readonly FastList<PtrSize> _values = new FastList<PtrSize>();
        private Transaction _currentTransaction;
        private int _elementSize = 1;
        private bool _isDirty;
        private ByteString _compressed, _raw;
        private ZstdLib.CompressionDictionary _previousDictionary;
        private int _size;
        private ByteStringContext<ByteStringMemoryCache>.InternalScope _compressedScope, _rawScope;
        public bool Compressed => _compressed.HasValue;

        public byte CompressionRatio
        {
            get
            {
                if (_compressed.HasValue == false)
                    throw new InvalidOperationException("Cannot compute compression ratio if not compressed");

                var result = (byte)((_compressed.Length / (float)_raw.Length) * 100);
                return result;
            }
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

        public int SizeLarge
        {
            get
            {
                if (_compressed.HasValue)
                    return _compressed.Length + _previousDictionary.Hash.Size;
                
                return Size;
            }
        }

        public int Size
        {
            get
            {
                if (_compressed.HasValue)
                    return _compressed.Length;

                return _size + ElementSize * _values.Count + JsonParserState.VariableSizeIntSize(_values.Count);
            }
        }

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
            _currentTransaction = null;
            
            ResetCompression();
        }

        private void ResetCompression()
        {
            _compressed = default;
            _raw = default;
            _rawScope.Dispose();
            _compressedScope.Dispose();
            _previousDictionary = null;
        }

        public void SetCurrentTransaction(Transaction value)
        {
            _currentTransaction = value;
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

        public bool ShouldReplaceDictionary(ZstdLib.CompressionDictionary newDic)
        {
            int maxSpace = ZstdLib.GetMaxCompression(_raw.Length);
            //TODO: Handle encrypted buffer here if encrypted

            var newCompressBufferScope = _currentTransaction.Allocator.Allocate(maxSpace, out var newCompressBuffer);
            try
            {
                var size = ZstdLib.Compress(_raw.ToReadOnlySpan(), newCompressBuffer.ToSpan(), newDic);
                // we want to be conservative about changing dictionaries, we'll only replace it if there
                // is a > 10% change in the data
                if (size >= _compressed.Length - (_compressed.Length/10))
                {
                    // couldn't get better rate, abort and use the current one
                    newCompressBufferScope.Dispose();
                    return false;
                }

                newCompressBuffer.Truncate(size);
                _compressedScope.Dispose();
                _compressed = newCompressBuffer;
                _compressedScope = newCompressBufferScope;
                return true;
            }
            catch 
            {
                newCompressBufferScope.Dispose();
                throw;
            }

        }
        
        public void TryCompression(ZstdLib.CompressionDictionary compressionDictionary)
        {
            if (_compressed.HasValue)
            {
                if (_previousDictionary == compressionDictionary)
                {
                    // we might be called first from update and then from insert, if the same dictionary is used, great, nothing to do
                    return;
                }
                // different dictionaries are used, need to re-compress
                ResetCompression();
            }

            _previousDictionary = compressionDictionary;

            int actualSize = Size;
            int maxSpace = ZstdLib.GetMaxCompression(actualSize);
            //TODO: Handle encrypted buffer here if encrypted
            _rawScope  = _currentTransaction.Allocator.Allocate(actualSize, out _raw);

            CopyTo(_raw.Ptr);

            _compressedScope = _currentTransaction.Allocator.Allocate(maxSpace, out var compressed);

            try
            {
                var size = ZstdLib.Compress(_raw.ToReadOnlySpan(), compressed.ToSpan(), compressionDictionary);
                const int sizeOfHash = 32;
                if (size + sizeOfHash >= _raw.Length)
                {
                    // we compressed large, so we skip compression here
                    _compressedScope.Dispose();
                    _rawScope.Dispose();
                    return;
                }

                _compressed = compressed;
                _compressed.Truncate(size);
            }
            catch 
            {
                _compressedScope.Dispose();
                _rawScope.Dispose();
                throw;
            }

        }

        public void CopyToLarge(byte* ptr)
        {
            if (_compressed.HasValue)
            {
                _previousDictionary.Hash.CopyTo(ptr);
                ptr += _previousDictionary.Hash.Size;
            }
            CopyTo(ptr);
        }

        public void CopyTo(byte* ptr)
        {
            if (_compressed.HasValue)
            {
                _compressed.CopyTo(ptr);
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

        public TableValueReader CreateReader(byte* pos)
        {
            return _compressed.HasValue ? new TableValueReader(_raw.Ptr, _raw.Length) : new TableValueReader(pos, Size);
        }
    }
}
