using System;
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
        
        private struct DefaultList : IReadOnlySpanEnumerator
        {
            public int Length => 2;

            public ReadOnlySpan<byte> this[int i] => new byte[] { (byte)(65 +i), (byte)(66 +i), (byte)(67 +i)};
        }

        public static  long CreateEmpty(LowLevelTransaction llt)
        {
            var p = llt.AllocatePage(NumberOfPagesForDictionary);
            p.Flags = PageFlags.Overflow;
            p.OverflowSize = UsableDictionarySize;
            //TODO: What would be the default here?
            //var encoder = new HopeEncoder<Encoder3Gram>();
            //encoder.Train(new NativeMemoryEncoderState(p.DataPointer, UsableDictionarySize), new DefaultList(), UsableDictionarySize);
            return p.PageNumber;
        }

        public PersistentHopeDictionary(Page page)
        {
            _page = page;
        }

        public void Decode(ReadOnlySpan<byte> encodedKey, ref Span<byte> decodedKey)
        {
            for (int i = 0; i < encodedKey.Length - 3; i++)
            {
                if ((byte)(((encodedKey[i] | 32) - (byte)'a')) < 26)
                {
                    decodedKey[i] = (byte)(encodedKey[i] ^ 32);
                }
                else
                {
                    decodedKey[i] = encodedKey[i];
                }
            }
            decodedKey = decodedKey.Slice(0, encodedKey.Length - (int)(_page.PageNumber % 6));
        }

        public void Encode(ReadOnlySpan<byte> key, ref Span<byte> encodedKey)
        {
            // simulate encoding
            for (int i = 0; i < key.Length; i++)
            {
                if ((byte)(((key[i] | 32) - (byte)'a')) < 26)
                {
                    encodedKey[i] = (byte)(key[i] ^ 32);
                }
                else
                {
                    encodedKey[i] = key[i];
                }
            }
            for (int i = 0; i < _page.PageNumber % 6; i++)
            {
                encodedKey[key.Length + i] = (byte)((byte)'a' + (byte)i);
            }
            encodedKey = encodedKey.Slice(0, key.Length + (int)(_page.PageNumber % 6));
            //var encoder = new HopeEncoder<Encoder3Gram>();
            //var state = new NativeMemoryEncoderState(_page.DataPointer, UsableDictionarySize);
            //var len = encoder.Encode(state, key, encodedKey);
            //encodedKey = encodedKey.Slice(0, len);
        }
    }
}
