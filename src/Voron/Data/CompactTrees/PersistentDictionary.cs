using System;
using System.Buffers;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Sparrow;
using Sparrow.Binary;
using Sparrow.Server.Compression;
using Voron.Exceptions;
using Voron.Global;
using Voron.Impl;
using Voron.Impl.Paging;
using static Sparrow.Hashing;

namespace Voron.Data.CompactTrees
{
    [StructLayout(LayoutKind.Explicit, Pack = 1)]
    public struct PersistentDictionaryRootHeader
    {
        [FieldOffset(0)]
        public RootObjectType RootObjectType;

        [FieldOffset(1)]
        public long PageNumber;
    }
    
    [StructLayout(LayoutKind.Explicit, Pack = 1)]
    public struct PersistentDictionaryHeader
    {
        public const int SizeOf = 32;

        [FieldOffset(0)]        
        public ulong TableHash;
        [FieldOffset(8)]
        public int TableSize;
        [FieldOffset(16)]
        public long CurrentId;
        [FieldOffset(24)]
        public long Reserved;


        public override string ToString()
        {
            return $"{nameof(TableHash)}: {TableHash}, {nameof(TableSize)}: {TableSize}, {nameof(CurrentId)}: {CurrentId}";
        }
    }

    public unsafe partial class PersistentDictionary
    {
        public const int MaxDictionaryEntriesForTraining = 8000;

        public const string DictionaryKey = $"{nameof(PersistentDictionary)}.Current";
        public readonly long DictionaryId;
        
        private readonly HopeEncoder<Encoder3Gram<AdaptiveMemoryEncoderState>> _encoder;

        public static long CreateDefault(LowLevelTransaction llt)
        {
            using var _ = Slice.From(llt.Allocator, DictionaryKey, out var defaultKey);

            long pageNumber;

            var result = llt.RootObjects.DirectRead(defaultKey);
            if (result != null)
            {
                pageNumber = ((PersistentDictionaryRootHeader*)result)->PageNumber;
            }
            else
            {
                var numberOfPages = Paging.GetNumberOfOverflowPages(DefaultAllocationSizeForTable);
                var p = llt.AllocatePage(numberOfPages);
                p.Flags = PageFlags.Overflow;
                p.OverflowSize = DefaultAllocationSizeForTable;

                PersistentDictionaryHeader* header = (PersistentDictionaryHeader*)p.DataPointer;
                header->TableSize = DefaultDictionaryTableSize;
                header->CurrentId = p.PageNumber;

                // We retrieve the embedded file from the assembly, copy and checksum the entire thing.             
                var embeddedFile = typeof(PersistentDictionary).Assembly.GetManifestResourceStream($"Voron.Data.CompactTrees.dictionary.bin");
                if (embeddedFile == null)
                    VoronUnrecoverableErrorException.Raise(llt.Environment, "The default dictionary has not been included in the build, the build process needs to be corrected.");

                var dictionary = new byte[embeddedFile.Length];
                int unused = embeddedFile.Read(dictionary);

                var arrayAsInt = MemoryMarshal.Cast<byte, int>(dictionary.AsSpan());
                int tableSize = arrayAsInt[0];
                if (tableSize != DefaultDictionaryTableSize)
                    VoronUnrecoverableErrorException.Raise(llt.Environment, "There is an inconsistency between the expected size of the default compression dictionary and the one read from storage.");

                dictionary.AsSpan()
                    .Slice(4) // Discard the table size.
                    .CopyTo(new Span<byte>(p.DataPointer + sizeof(PersistentDictionaryHeader), DefaultDictionaryTableSize));

                header->TableHash = XXHash64.Calculate(p.DataPointer + sizeof(long), DefaultDictionaryTableSize + PersistentDictionaryHeader.SizeOf - sizeof(long));

#if DEBUG
                VerifyTable(p);
#endif

                pageNumber = p.PageNumber;

                using var scope = llt.RootObjects.DirectAdd(defaultKey, sizeof(PersistentDictionaryRootHeader), out var ptr);
                *(PersistentDictionaryRootHeader*)ptr = new PersistentDictionaryRootHeader()
                {
                    RootObjectType = RootObjectType.PersistentDictionary,
                    PageNumber = pageNumber
                };
            }

            return pageNumber;
        }

        public const int MinSamplesToTrain = 500;

        public static bool TryCreate<TKeys1>(LowLevelTransaction llt, TKeys1 trainEnumerator, out PersistentDictionary dictionary)
            where TKeys1 : IReadOnlySpanEnumerator
        {
            var encoderState = new AdaptiveMemoryEncoderState();
            using var encoder = new HopeEncoder<Encoder3Gram<AdaptiveMemoryEncoderState>>(new Encoder3Gram<AdaptiveMemoryEncoderState>(encoderState));
            encoder.Train(trainEnumerator, MaxDictionaryEntriesForTraining);

            int requiredSize = Encoder3Gram<AdaptiveMemoryEncoderState>.GetDictionarySize(encoderState);
            
            // Check if the actual encoding table has enough diversity so that it makes sense to be using it.
            // If there is not enough diversity it means that we are probably overfitting to a single page,
            // so whatever we do it is most likely going to be less efficient than the default dictionary. 
            if (trainEnumerator.Count < MinSamplesToTrain)
            {
                dictionary = null;
                return false;
            }

            int requiredTotalSize = requiredSize + PersistentDictionaryHeader.SizeOf;
            var numberOfPages = Paging.GetNumberOfOverflowPages(requiredTotalSize);
            var p = llt.AllocatePage(numberOfPages);
            p.Flags = PageFlags.Overflow;
            p.OverflowSize = requiredTotalSize;           

            PersistentDictionaryHeader* header = (PersistentDictionaryHeader*)p.DataPointer;
            header->CurrentId = p.PageNumber;

            byte* encodingTablesPtr = p.DataPointer + PersistentDictionaryHeader.SizeOf;
            encoderState.EncodingTable.Slice(0, requiredSize / 2).CopyTo(new Span<byte>(encodingTablesPtr, requiredSize / 2));
            encoderState.DecodingTable.Slice(0, requiredSize / 2).CopyTo(new Span<byte>(encodingTablesPtr + requiredSize / 2, requiredSize / 2));

            header->TableSize = requiredSize;
            header->TableHash = XXHash64.Calculate(p.DataPointer + sizeof(ulong), (ulong) (header->TableSize + PersistentDictionaryHeader.SizeOf - sizeof(ulong)));

#if DEBUG
            VerifyTable(p);
#endif

            using var _ = Slice.From(llt.Allocator, DictionaryKey, out var defaultKey);
            using var scope = llt.RootObjects.DirectAdd(defaultKey, sizeof(PersistentDictionaryRootHeader), out var ptr);
            *(PersistentDictionaryRootHeader*)ptr = new PersistentDictionaryRootHeader()
            {
                RootObjectType = RootObjectType.PersistentDictionary,
                PageNumber = p.PageNumber
            };

            dictionary = new PersistentDictionary(p);
            return true;
        }

        public static void VerifyTable(Page page)
        {
            PersistentDictionaryHeader* header = (PersistentDictionaryHeader*)page.DataPointer;

            // If checksum is not correct, we will throw a corruption exception
            var tableHash = XXHash64.Calculate(page.DataPointer + sizeof(long), (ulong)header->TableSize + PersistentDictionaryHeader.SizeOf - sizeof(long));
            if (tableHash != header->TableHash)
                throw new VoronErrorException($"Persistent storage checksum mismatch, expected: {tableHash}, actual: {header->TableHash}");

            // In the future for recovery scenarios we should do more advanced encoding/decoding verifications to ensure the table is not corrupted. 
            // https://issues.hibernatingrhinos.com/issue/RavenDB-18547
        }

        public PersistentDictionary(Page page)
        {
            DictionaryId = page.PageNumber;

            PersistentDictionaryHeader* header = (PersistentDictionaryHeader*)page.DataPointer;

            byte* startPtr = page.DataPointer + PersistentDictionaryHeader.SizeOf;
            int tableSize = header->TableSize;

            var nativeState = new NativeMemoryEncoderState(startPtr, tableSize);
            var managedState = new AdaptiveMemoryEncoderState(tableSize);

            nativeState.EncodingTable.CopyTo(managedState.EncodingTable);
            nativeState.DecodingTable.CopyTo(managedState.DecodingTable);

            _encoder = new HopeEncoder<Encoder3Gram<AdaptiveMemoryEncoderState>>(
                new Encoder3Gram<AdaptiveMemoryEncoderState>(managedState));
        }

        public void Decode(int keyLengthInBits, ReadOnlySpan<byte> key, ref Span<byte> decodedKey)
        {
            int len = _encoder.Decode(keyLengthInBits, key, decodedKey);
            decodedKey = decodedKey.Slice(0, len);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Encode(ReadOnlySpan<byte> key, ref Span<byte> encodedKey, out int encodedKeyLengthInBits)
        {
            if (key.Length == 0)
                throw new ArgumentException("Cannot encode an empty key!", nameof(key));

            encodedKeyLengthInBits = _encoder.Encode(key, encodedKey);
            encodedKey = encodedKey.Slice(0, Bits.ToBytes(encodedKeyLengthInBits));
        }

        public int GetMaxEncodingBytes(int keyLength)
        {
            // The plus one is because we may be sending non null terminated strings and we have to account for it. 
            return Math.Max(sizeof(long), _encoder.GetMaxEncodingBytes(keyLength + 1));
        }

        public int GetMaxDecodingBytes(int keyLength)
        {
            return _encoder.GetMaxDecodingBytes(keyLength);
        }
    }
}
