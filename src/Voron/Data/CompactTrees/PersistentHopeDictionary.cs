using System;
using System.Buffers;
using System.Linq;
using Sparrow;
using Sparrow.Server.Compression;
using Voron.Global;
using Voron.Impl;

namespace Voron.Data.CompactTrees
{
    public unsafe class PersistentHopeDictionary 
    {
        private readonly Page _page;
        private const int NumberOfPagesForDictionary = 8;
        public const int UsableDictionarySize = 8 * Constants.Storage.PageSize - PageHeader.SizeOf;

        public long PageNumber => _page.PageNumber;
        
        private readonly HopeEncoder<Encoder3Gram<NativeMemoryEncoderState>> _encoder;
        private byte[] _tempBuffer;

        private struct DefaultList : IReadOnlySpanEnumerator
        {
            public int Length => 0;

            public ReadOnlySpan<byte> this[int i] => throw new NotImplementedException();
        }

        public static long CreateEmpty(LowLevelTransaction llt)
        {
            var p = llt.AllocatePage(NumberOfPagesForDictionary);
            p.Flags = PageFlags.Overflow;
            p.OverflowSize = UsableDictionarySize;

            var encoder = new HopeEncoder<Encoder3Gram<NativeMemoryEncoderState>>(
                new Encoder3Gram<NativeMemoryEncoderState>(
                    new NativeMemoryEncoderState(p.DataPointer, UsableDictionarySize)));
            encoder.Train(new DefaultList(), 128);

            return p.PageNumber;
        }

        public static PersistentHopeDictionary Create<TKeysEnumerator>(LowLevelTransaction llt, in TKeysEnumerator enumerator)
            where TKeysEnumerator : struct, IReadOnlySpanEnumerator
        {
            var p = llt.AllocatePage(NumberOfPagesForDictionary);
            p.Flags = PageFlags.Overflow;
            p.OverflowSize = UsableDictionarySize;

            var encoder = new HopeEncoder<Encoder3Gram<NativeMemoryEncoderState>>(
                new Encoder3Gram<NativeMemoryEncoderState>(
                    new NativeMemoryEncoderState(p.DataPointer, UsableDictionarySize)));
            encoder.Train(enumerator, 128);

            return new PersistentHopeDictionary(p);
        }

        public PersistentHopeDictionary(Page page)
        {
            _page = page;

            _encoder = new HopeEncoder<Encoder3Gram<NativeMemoryEncoderState>>(
                new Encoder3Gram<NativeMemoryEncoderState>(
                    new NativeMemoryEncoderState(page.DataPointer, UsableDictionarySize)));
        }

        public void Decode(ReadOnlySpan<byte> encodedKey, ref Span<byte> decodedKey)
        {
            int len = _encoder.Decode(encodedKey, decodedKey);
            decodedKey = decodedKey.Slice(0, len);
        }

        public void Encode(ReadOnlySpan<byte> key, ref Span<byte> encodedKey)
        {
            if (key.Length == 0)
                throw new ArgumentException();

            if (key[^1] != 0)
            {
                if (_tempBuffer == null || _tempBuffer.Length < key.Length + 1)
                {
                    if (_tempBuffer != null)
                        ArrayPool<byte>.Shared.Return(_tempBuffer);
                    _tempBuffer = ArrayPool<byte>.Shared.Rent(key.Length + 1);
                }

                var newKey = _tempBuffer.AsSpan();
                key.CopyTo(newKey);
                newKey[key.Length] = 0;
                key = newKey.Slice(0, key.Length + 1);
            }
            
            int bitsLength = _encoder.Encode(key, encodedKey);
            int bytesLength = Math.DivRem(bitsLength, 8, out var remainder);
            encodedKey = encodedKey.Slice(0, bytesLength + (remainder == 0 ? 0 : 1));
        }

        public int GetMaxEncodingBytes(ReadOnlySpan<byte> key)
        {
            // The plus one is because we may be sending non null terminated strings and we have to account for it. 
            return Math.Max(sizeof(long), _encoder.GetMaxEncodingBytes(key.Length + 1));
        }

        public int GetMaxDecodingBytes(ReadOnlySpan<byte> key)
        {
            return _encoder.GetMaxDecodingBytes(key.Length);
        }
    }
}
