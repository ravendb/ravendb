using System;
using System.Buffers;
using System.Runtime.InteropServices;
using Sparrow;
using Sparrow.Server.Compression;
using Voron.Exceptions;
using Voron.Global;
using Voron.Impl;
using static Sparrow.Hashing;

namespace Voron.Data.CompactTrees
{
    [StructLayout(LayoutKind.Explicit, Pack = 1)]
    public unsafe struct PersistentDictionaryHeader
    {
        public const int SizeOf = 32;

        [FieldOffset(0)]        
        public ulong TableHash;
        [FieldOffset(8)]
        public long CurrentId;
        [FieldOffset(16)]
        public long PreviousId;

        public override string ToString()
        {
            return $"{nameof(TableHash)}: {TableHash}, {nameof(CurrentId)}: {CurrentId}, {nameof(PreviousId)}: {PreviousId}";
        }
    }

    public unsafe partial class PersistentDictionary 
    {
        private readonly Page _page;
        
        public const int UsableDictionarySize = NumberOfPagesForDictionary * Constants.Storage.PageSize - PageHeader.SizeOf - PersistentDictionaryHeader.SizeOf;

        public long PageNumber => _page.PageNumber;
        
        private readonly HopeEncoder<Encoder3Gram<NativeMemoryEncoderState>> _encoder;
        private byte[] _tempBuffer;

        public static long CreateDefault(LowLevelTransaction llt)
        {
            var p = llt.AllocatePage(NumberOfPagesForDictionary);
            p.Flags = PageFlags.Overflow;
            p.OverflowSize = NumberOfPagesForDictionary * Constants.Storage.PageSize;

            PersistentDictionaryHeader* header = (PersistentDictionaryHeader*)p.DataPointer;
            header->CurrentId = p.PageNumber;
            header->PreviousId = 0;

            // We retrieve the embeeded file from the assembly, copy and checksum the entire thing. 
            var dictionary = new byte[UsableDictionarySize];
            var embeddedFile = typeof(PersistentDictionary).Assembly.GetManifestResourceStream($"Voron.Data.CompactTrees.dictionary.bin");
            embeddedFile.Read(dictionary);
            dictionary.CopyTo(new Span<byte>(p.DataPointer + sizeof(PersistentDictionaryHeader), UsableDictionarySize));

            header->TableHash = XXHash64.Calculate(p.DataPointer + sizeof(long), UsableDictionarySize + PersistentDictionaryHeader.SizeOf - sizeof(long));

#if DEBUG
            VerifyTable(p);
#endif

            return p.PageNumber;
        }

        public static PersistentDictionary Create<TKeysEnumerator>(LowLevelTransaction llt, in TKeysEnumerator enumerator, PersistentDictionary previousDictionary = null)
            where TKeysEnumerator : struct, IReadOnlySpanEnumerator
        {
            var p = llt.AllocatePage(NumberOfPagesForDictionary);
            p.Flags = PageFlags.Overflow;
            p.OverflowSize = NumberOfPagesForDictionary * Constants.Storage.PageSize;

            PersistentDictionaryHeader* header = (PersistentDictionaryHeader*)p.DataPointer;
            header->CurrentId = p.PageNumber;
            header->PreviousId = previousDictionary != null ? previousDictionary.PageNumber : 0;
            
            var encoder = new HopeEncoder<Encoder3Gram<NativeMemoryEncoderState>>(
                new Encoder3Gram<NativeMemoryEncoderState>(
                    new NativeMemoryEncoderState(p.DataPointer + sizeof(PersistentDictionaryHeader), UsableDictionarySize)));
            encoder.Train(enumerator, MaxDictionarySize);

            header->TableHash = XXHash64.Calculate(p.DataPointer + sizeof(long), UsableDictionarySize + PersistentDictionaryHeader.SizeOf - sizeof(long));

#if DEBUG
            VerifyTable(p);
#endif

            return new PersistentDictionary(p);
        }

        public static void VerifyTable(Page page)
        {
            var tableHash = XXHash64.Calculate(page.DataPointer + sizeof(long), UsableDictionarySize + PersistentDictionaryHeader.SizeOf - sizeof(long));
            PersistentDictionaryHeader* header = (PersistentDictionaryHeader*)page.DataPointer;

            // If checksume is not correct, we will throw a corruption exception
            if (tableHash != header->TableHash)
                throw new VoronErrorException($"Persistent storage checksum mismatch, expected: {tableHash}, actual: {header->TableHash}");
        }

        public PersistentDictionary(Page page)
        {
            _page = page;

            _encoder = new HopeEncoder<Encoder3Gram<NativeMemoryEncoderState>>(
                new Encoder3Gram<NativeMemoryEncoderState>(
                    new NativeMemoryEncoderState(page.DataPointer + sizeof(PersistentDictionaryHeader), UsableDictionarySize)));
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
