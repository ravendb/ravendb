using System;
using Sparrow.Server;
using Voron.Impl;

namespace Voron.Data.Tables
{
    public unsafe class TableValueCompressor
    {
        private readonly TableValueBuilder _builder;
        public ByteString CompressedBuffer, RawBuffer;
        private ZstdLib.CompressionDictionary _previousDictionary;
        public ByteStringContext<ByteStringMemoryCache>.InternalScope CompressedScope, RawScope;
        private Transaction _currentTransaction;

        public TableValueCompressor(TableValueBuilder builder)
        {
            _builder = builder;
        }

        public void SetCurrentTransaction(Transaction value)
        {
            _currentTransaction = value;
        }

        public TableValueReader CreateReader(byte* pos)
        {
            return CompressedBuffer.HasValue ? 
                new TableValueReader(RawBuffer.Ptr, RawBuffer.Length) : 
                new TableValueReader(pos, RawBuffer.Length);
        }

        public byte CompressionRatio
        {
            get
            {
                if (CompressedBuffer.HasValue == false)
                    throw new InvalidOperationException("Cannot compute compression ratio if not compressed");

                var result = (byte)((CompressedBuffer.Length / (float)RawBuffer.Length) * 100);
                return result;
            }
        }


        public bool Compressed;

        public bool TryCompression(ZstdLib.CompressionDictionary compressionDictionary)
        {
            if (Compressed)
            {
                if (_previousDictionary == compressionDictionary)
                {
                    // we might be called first from update and then from insert, if the same dictionary is used, great, nothing to do
                    return true;
                }
                // different dictionaries are used, need to re-compress
            }

            _previousDictionary = compressionDictionary;

            try
            {
                var size = ZstdLib.Compress(RawBuffer.ToReadOnlySpan(), CompressedBuffer.ToSpan(), compressionDictionary);
                const int sizeOfHash = 32;
                if (size + sizeOfHash >= RawBuffer.Length)
                {
                    // we compressed large, so we skip compression here
                    CompressedScope.Dispose();
                    // explicitly not releasing this, we may be still call this if we 
                    // are switching sections and re-training, so we want to keep it around
                    //RawScope.Dispose();
                    Compressed = false;
                    return false;
                }
                Compressed = true;
                CompressedBuffer.Truncate(size);
                return true;
            }
            catch
            {
                CompressedScope.Dispose();
                RawScope.Dispose();
                throw;
            }

        }


        public bool ShouldReplaceDictionary(ZstdLib.CompressionDictionary newDic)
        {
            int maxSpace = ZstdLib.GetMaxCompression(RawBuffer.Length);
            //TODO: Handle encrypted buffer here if encrypted

            var newCompressBufferScope = _currentTransaction.Allocator.Allocate(maxSpace, out var newCompressBuffer);
            try
            {
                var size = ZstdLib.Compress(RawBuffer.ToReadOnlySpan(), newCompressBuffer.ToSpan(), newDic);
                // we want to be conservative about changing dictionaries, we'll only replace it if there
                // is a > 10% change in the data
                if (Compressed && size >= CompressedBuffer.Length - (CompressedBuffer.Length / 10))
                {
                    // couldn't get better rate, abort and use the current one
                    newCompressBufferScope.Dispose();
                    return false;
                }
                Compressed = true;
                newCompressBuffer.Truncate(size);
                CompressedScope.Dispose();
                CompressedBuffer = newCompressBuffer;
                CompressedScope = newCompressBufferScope;
                _previousDictionary = newDic;
                return true;
            }
            catch
            {
                newCompressBufferScope.Dispose();
                throw;
            }

        }


        public void CopyToLarge(byte* ptr)
        {
            if (CompressedBuffer.HasValue)
            {
                _previousDictionary.Hash.CopyTo(ptr);
                ptr += _previousDictionary.Hash.Size;
            }
            CopyTo(ptr);
        }

        public void CopyTo(byte* ptr)
        {
            if (CompressedBuffer.HasValue)
            {
                CompressedBuffer.CopyTo(ptr);
                return;
            }

            RawBuffer.CopyTo(ptr);
        }

        public void DiscardCompressedData()
        {
            Compressed = false;
            CompressedBuffer = default;
            CompressedScope.Dispose();
            _previousDictionary = null;
        }

        public void Reset()
        {
            Compressed = false;
            RawBuffer = default;
            RawScope.Dispose();
            DiscardCompressedData();
        }

        public int SizeLarge
        {
            get
            {
                if (Compressed)
                    return CompressedBuffer.Length + _previousDictionary.Hash.Size;

                return RawBuffer.Length;
            }
        }

        public bool IsValid => RawBuffer.HasValue;
        public int Size
        {
            get
            {
                if (Compressed)
                {
                    return CompressedBuffer.Length;
                }

                if (RawBuffer.HasValue)
                {
                    return RawBuffer.Length;
                }

                return _builder.Size;
            }
        }

        public void Prepare(in int size)
        {
            int maxSpace = ZstdLib.GetMaxCompression(size);
            //TODO: Handle encrypted buffer here if encrypted
            RawScope = _currentTransaction.Allocator.Allocate(size, out RawBuffer);
            CompressedScope = _currentTransaction.Allocator.Allocate(maxSpace, out CompressedBuffer);
            _previousDictionary = null;
            Compressed = false;

        }

        public bool Redundant(ZstdLib.CompressionDictionary current)
        {
            return current == _previousDictionary;
        }

        public void SetDictionary(ZstdLib.CompressionDictionary dic)
        {
            _previousDictionary = dic;
        }
    }
}
