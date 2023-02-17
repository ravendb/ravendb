using System;
using System.Buffers.Binary;
using System.Diagnostics;
using System.IO;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics.X86;
using Sparrow.Collections;
using Sparrow.Server.Binary;
using Sparrow.Server.Collections.Persistent;
using IntPtr = System.IntPtr;

namespace Sparrow.Server.Compression
{
    [StructLayout(LayoutKind.Explicit)]
    internal unsafe struct Interval3Gram
    {
        [FieldOffset(0)]
        public uint BufferAndLength;

        [FieldOffset(0)]
        public fixed byte KeyBuffer[3];
        [FieldOffset(3)]
        public byte _prefixAndKeyLength;

        [FieldOffset(4)]
        public Code Code;

        public byte PrefixLength
        {
            get { return (byte)(_prefixAndKeyLength & 0x0F); }
            set { _prefixAndKeyLength = (byte)(_prefixAndKeyLength & 0xF0 | value & 0x0F); }
        }

        public byte KeyLength
        {
            get { return (byte)(_prefixAndKeyLength >> 4); }
            set { _prefixAndKeyLength = (byte)(_prefixAndKeyLength & 0x0F | (value << 4)); }
        }
    }

    public unsafe struct Encoder3Gram<TEncoderState> : IEncoderAlgorithm
        where TEncoderState : struct, IEncoderState
    {
        private readonly TEncoderState _state;
        private int _entries;

        public Encoder3Gram(TEncoderState state)
        {
            _state = state;
            _entries = ReadNumberOfEntries(state);
        }

        public static int GetDictionarySize(in TEncoderState state)
        {
            int entries = MemoryMarshal.Cast<byte, int>(state.EncodingTable.Slice(0, 4))[0];
            return ( entries * Unsafe.SizeOf<Interval3Gram>() + 8 ) * 2; // The encoding and the decoding table size.
        }

        public static int GetEntriesTableSize(in TEncoderState state)
        {
            return MemoryMarshal.Cast<byte, int>(state.EncodingTable.Slice(0, 4))[0];
        }

        public void Train<TSampleEnumerator>(in TSampleEnumerator enumerator, int dictionarySize)
            where TSampleEnumerator : struct, IReadOnlySpanEnumerator
        {
            var symbolSelector = new Encoder3GramSymbolSelector<TSampleEnumerator>();
            var frequencyList = symbolSelector.SelectSymbols(enumerator, dictionarySize);

            var codeAssigner = new HuTuckerCodeAssigner();            
            var symbolCodes = codeAssigner.AssignCodes(frequencyList);

            BuildDictionary(symbolCodes);
        }

        private string ToBinaryString(short sBuf, int sLen)
        {
            Span<short> value = stackalloc short[1];
            value[0] = BinaryPrimitives.ReverseEndianness((short)(sBuf << (sizeof(short) * 8 - sLen)));
            BitReader reader = new(MemoryMarshal.Cast<short, byte>(value), sLen);

            string result = string.Empty;
            while (reader.Length != 0)
            {
                if (reader.Read().IsSet)
                    result += "R";
                else
                    result += "L";
            }
            return result;
        }

        public void EncodeBatch<TSampleEnumerator, TOutputEnumerator>(in TSampleEnumerator data, Span<int> outputSizes, in TOutputEnumerator outputBuffers)
            where TSampleEnumerator : struct, IReadOnlySpanIndexer
            where TOutputEnumerator : struct, ISpanIndexer
        {
            fixed (Interval3Gram* table = EncodingTable)
            {
                var numberOfEntries = _entries;

                int length = data.Length;
                for (int i = 0; i < length; i++)
                {
                    var intBuf = MemoryMarshal.Cast<byte, long>(outputBuffers[i]);

                    int idx = 0;
                    intBuf[0] = 0;
                    int intBufLen = 0;

                    var bitWriterBuffer = intBuf[idx];

                    var key = data[i];

                    fixed (byte* fixedKey = key)
                    {
                        int keyLength = key.Length;
                        byte* keyPtr = fixedKey;

                        while (keyLength > 0)
                        {
                            // Initialize the important variables.
                            uint symbolValue = 0;
                            if (keyLength >= 4)
                            {
                                // PERF: This is the usual case, this is the branch that gets executed for most of the
                                // key, at least until we hit the tail.
                                symbolValue = BinaryPrimitives.ReverseEndianness(*(uint*)keyPtr) >> 8;
                            }
                            else
                            {
                                // PERF: we are performing masked copy with reverse endianness using tables, with
                                // the objective of diminishing the amount of instructions necessary for execution.
                                // The switch implementation requires over 40 instructions including several branches,
                                // this version is much more succinct and do not need any jump. 

                                // We copy to the symbols buffer the part of the key we will be consuming.
                                int tableIndex = keyLength * 4;

                                symbolValue = (uint)(keyPtr[EncodingTailTable[tableIndex]] << 16 |
                                                     keyPtr[EncodingTailTable[tableIndex + 1]] << 8 |
                                                     keyPtr[EncodingTailTable[tableIndex + 2]]);

                                symbolValue &= 0xFFFFFFFF << EncodingTailMaskTable[keyLength];
                            }

                            int prefixLen = Lookup(symbolValue, table, numberOfEntries, out Code code);


                            // Perform the actual encoding in a bitwise fashion. 
                            long sBuf = code.Value;
                            int sLen = code.Length;
                            if (intBufLen + sLen > 63)
                            {
                                int numBitsLeft = 64 - intBufLen;
                                intBufLen = sLen - numBitsLeft;

                                intBuf[idx] = BinaryPrimitives.ReverseEndianness((bitWriterBuffer << numBitsLeft) | (sBuf >> intBufLen));
                                idx++;
                                bitWriterBuffer = sBuf;
                            }
                            else
                            {
                                bitWriterBuffer = (bitWriterBuffer << sLen) | sBuf;
                                intBufLen += sLen;
                            }

                            keyLength -= prefixLen;
                            keyPtr += prefixLen;
                        }

                        intBuf[idx] = BinaryPrimitives.ReverseEndianness(bitWriterBuffer << (64 - intBufLen));
                        outputSizes[i] = ((idx << 6) + intBufLen);
                    }
                }
            }
        }

        private static ReadOnlySpan<byte> EncodingTailTable => new byte[]
        {
            0, 0, 0, 0,
            0, 0, 0, 0,
            0, 1, 0, 0,
            0, 1, 2, 0
        };

        private static ReadOnlySpan<byte> EncodingTailMaskTable => new byte[] { 32, 16, 8, 0 };


        public void DecodeBatch<TSampleEnumerator, TOutputEnumerator>(in TSampleEnumerator data, Span<int> outputSize, in TOutputEnumerator outputBuffers)
            where TSampleEnumerator : struct, IReadOnlySpanIndexer
            where TOutputEnumerator : struct, ISpanIndexer
        {
            ref Interval3Gram tableRef = ref EncodingTable[0];
            var tree = BinaryTree<short>.Open(_state.DecodingTable);

            byte* auxBuffer = stackalloc byte[4];
            for (int i = 0; i < data.Length; i++)
            {
                ref byte symbolsPtrRef = ref MemoryMarshal.GetReference(outputBuffers[i]);
                ref byte dataRef = ref MemoryMarshal.GetReference(data[i]);
                int bitStreamLength = data[i].Length * 8; // We convert the length to bits.

                int readerBitIdx = 0;
                nuint offset = 0;

                while (readerBitIdx < bitStreamLength)
                {
                    // This is a fundamental change on how the HOPE decoding process works. By means of removing the NGRAMs that
                    // include NULL characters also known as byte(0) and setting the frequency of the NULL terminator when found to zero
                    // we force the use a whole byte (or more) for representation.
                    // 
                    // Since in Corax we don't store the bit length but the length of the stored key; therefore the decoder
                    // will not know when the bit stream end. For that we are using the property that the NULL character encoding
                    // takes more than a full byte (8 bits) with all 0 bits in it. Since the encoding process ensures that unused bits are
                    // all 0s, then we can at decoding time figure out that we have reach the end of the encoded stream. This is 
                    // done knowing that we get an invalid prefix but we have less than 8 bits in the stream.
                    // https://issues.hibernatingrhinos.com/issue/RavenDB-19703
                    var currentReaderBitIdx = tree.FindCommonPrefix(ref dataRef, bitStreamLength, readerBitIdx, out var idx);
                    if (currentReaderBitIdx == 0)
                    {
                        if ((bitStreamLength - readerBitIdx) < 8)
                            break;
                        else
                            goto Fail;
                    }

                    readerBitIdx = currentReaderBitIdx;

                    var p = Unsafe.AddByteOffset(ref tableRef, (IntPtr)(idx * Unsafe.SizeOf<Interval3Gram>()));

                    int prefixLength = p.PrefixLength;
                    *(uint*)auxBuffer = p.BufferAndLength;

                    if (prefixLength == 1)
                    {
                        Unsafe.WriteUnaligned(ref Unsafe.AddByteOffset(ref symbolsPtrRef, offset), auxBuffer[0]);
                    }
                    else if (prefixLength == 2)
                    {
                        Unsafe.WriteUnaligned(ref Unsafe.AddByteOffset(ref symbolsPtrRef, offset), auxBuffer[0]);
                        Unsafe.WriteUnaligned(ref Unsafe.AddByteOffset(ref symbolsPtrRef, offset + 1), auxBuffer[1]);
                    }
                    else
                    {
                        Unsafe.WriteUnaligned(ref Unsafe.AddByteOffset(ref symbolsPtrRef, offset), auxBuffer[0]);
                        Unsafe.WriteUnaligned(ref Unsafe.AddByteOffset(ref symbolsPtrRef, offset + 1), auxBuffer[1]);
                        Unsafe.WriteUnaligned(ref Unsafe.AddByteOffset(ref symbolsPtrRef, offset + 2), auxBuffer[2]);
                    }

                    offset += (nuint)prefixLength;
                }

                outputSize[i] = (int)offset;
            }

            Fail:
            throw new IOException("Invalid data stream.");
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Encode(ReadOnlySpan<byte> key, Span<byte> outputBuffer)
        {
            Debug.Assert(outputBuffer.Length >= sizeof(long)); // Ensure we can safely cast to int 64

            var intBuf = MemoryMarshal.Cast<byte, long>(outputBuffer);

            int idx = 0;
            intBuf[0] = 0;
            int intBufLen = 0;

            fixed(Interval3Gram* table = EncodingTable)
            fixed(byte* fixedKey = key)
            {
                var numberOfEntries = _entries;

                var keyPtr = fixedKey;
                var keyLength = key.Length;
                var bitWriterBuffer = intBuf[idx];

                while (keyLength > 0)
                {
                    // Initialize the important variables.
                    uint symbolValue = 0;
                    if (keyLength >= 4)
                    {
                        // PERF: This is the usual case, this is the branch that gets executed for most of the
                        // key, at least until we hit the tail.
                        symbolValue = BinaryPrimitives.ReverseEndianness(*(uint*)keyPtr) >> 8;
                    }
                    else
                    {
                        // PERF: we are performing masked copy with reverse endianness using tables, with
                        // the objective of diminishing the amount of instructions necessary for execution.
                        // The switch implementation requires over 40 instructions including several branches,
                        // this version is much more succinct and do not need any jump. 

                        // We copy to the symbols buffer the part of the key we will be consuming.
                        int tableIndex = keyLength * 4;

                        symbolValue = (uint)(keyPtr[EncodingTailTable[tableIndex]] << 16 |
                                             keyPtr[EncodingTailTable[tableIndex + 1]] << 8 |
                                             keyPtr[EncodingTailTable[tableIndex + 2]]);

                        symbolValue &= 0xFFFFFFFF << EncodingTailMaskTable[keyLength];
                    }

                    int prefixLen = Lookup(symbolValue, table, numberOfEntries, out Code code);


                    // Perform the actual encoding in a bitwise fashion. 
                    long sBuf = code.Value;
                    int sLen = code.Length;
                    if (intBufLen + sLen > 63)
                    {
                        int numBitsLeft = 64 - intBufLen;
                        intBufLen = sLen - numBitsLeft;

                        intBuf[idx] = BinaryPrimitives.ReverseEndianness((bitWriterBuffer << numBitsLeft) | (sBuf >> intBufLen));
                        idx++;
                        bitWriterBuffer = sBuf;
                    }
                    else
                    {
                        bitWriterBuffer = (bitWriterBuffer << sLen) | sBuf;
                        intBufLen += sLen;
                    }

                    keyLength -= prefixLen;
                    keyPtr += prefixLen;
                }

                intBuf[idx] = BinaryPrimitives.ReverseEndianness(bitWriterBuffer << (64 - intBufLen));
                return (idx << 6) + intBufLen;
            }
        }

        [SkipLocalsInit]
        public int Decode(int bitStreamLength, ReadOnlySpan<byte> data, Span<byte> outputBuffer)
        {
            ref Interval3Gram tableRef = ref EncodingTable[0];

            var tree = BinaryTree<short>.Open(_state.DecodingTable);
            byte* auxBuffer = stackalloc byte[4];

            ref byte symbolsPtrRef = ref outputBuffer[0];
            ref byte dataRef = ref MemoryMarshal.GetReference(data);
            int readerBitIdx = 0;
            nuint offset = 0;

            while (readerBitIdx < bitStreamLength)
            {
                // This is a fundamental change on how the HOPE decoding process works. By means of removing the NGRAMs that
                // include NULL characters also known as byte(0) and setting the frequency of the NULL terminator when found to zero
                // we force the use a whole byte (or more) for representation.
                // 
                // Since in Corax we don't store the bit length but the length of the stored key; therefore the decoder
                // will not know when the bit stream end. For that we are using the property that the NULL character encoding
                // takes more than a full byte (8 bits) with all 0 bits in it. Since the encoding process ensures that unused bits are
                // all 0s, then we can at decoding time figure out that we have reach the end of the encoded stream. This is 
                // done knowing that we get an invalid prefix but we have less than 8 bits in the stream.
                // https://issues.hibernatingrhinos.com/issue/RavenDB-19703
                var currentReaderBitIdx = tree.FindCommonPrefix(ref dataRef, bitStreamLength, readerBitIdx, out var idx);
                if (currentReaderBitIdx == 0)
                {
                    if ((bitStreamLength - readerBitIdx) < 8)
                        break;
                    else
                        goto Fail;
                }

                readerBitIdx = currentReaderBitIdx;

                var p = Unsafe.AddByteOffset(ref tableRef, (IntPtr)(idx * Unsafe.SizeOf<Interval3Gram>()));

                int prefixLength = p.PrefixLength;
                *(uint*)auxBuffer = p.BufferAndLength;

                if (prefixLength == 1)
                {
                    Unsafe.WriteUnaligned(ref Unsafe.AddByteOffset(ref symbolsPtrRef, offset), auxBuffer[0]);
                }
                else if (prefixLength == 2)
                {
                    Unsafe.WriteUnaligned(ref Unsafe.AddByteOffset(ref symbolsPtrRef, offset), auxBuffer[0]);
                    Unsafe.WriteUnaligned(ref Unsafe.AddByteOffset(ref symbolsPtrRef, offset + 1), auxBuffer[1]);
                }
                else
                {
                    Unsafe.WriteUnaligned(ref Unsafe.AddByteOffset(ref symbolsPtrRef, offset), auxBuffer[0]);
                    Unsafe.WriteUnaligned(ref Unsafe.AddByteOffset(ref symbolsPtrRef, offset + 1), auxBuffer[1]);
                    Unsafe.WriteUnaligned(ref Unsafe.AddByteOffset(ref symbolsPtrRef, offset + 2), auxBuffer[2]);
                }

                offset += (nuint)prefixLength;
            }

            return (int)offset;

            Fail:
            throw new IOException("Invalid data stream.");
        }

        public int NumberOfEntries
        {
            get { return ReadNumberOfEntries(_state); }
            set { MemoryMarshal.Write(_state.EncodingTable, ref value);}
        }

        private static int ReadNumberOfEntries(in TEncoderState encoderState)
        {
            return MemoryMarshal.Read<int>(encoderState.EncodingTable);
        }

        public int MemoryUse => NumberOfEntries * Unsafe.SizeOf<Interval3Gram>();

        public int MaxBitSequenceLength
        {
            get { return MemoryMarshal.Read<byte>(_state.EncodingTable.Slice(4, 1)); }
            set
            {
                byte valueAsByte = (byte)value;
                MemoryMarshal.Write(_state.EncodingTable.Slice(4, 1), ref valueAsByte);
            }
        }

        public int MinBitSequenceLength
        {
            get { return MemoryMarshal.Read<byte>(_state.EncodingTable.Slice(5, 1)); }
            set
            {
                byte valueAsByte = (byte)value;
                MemoryMarshal.Write(_state.EncodingTable.Slice(5, 1), ref valueAsByte);
            }
        }

        private Span<Interval3Gram> EncodingTable => MemoryMarshal.Cast<byte, Interval3Gram>(_state.EncodingTable.Slice(8));

        private unsafe void BuildDictionary(in FastList<SymbolCode> symbolCodeList)
        {
            EncodingTable.Clear(); // Zero out the memory we are going to be using. 

            // Creating the Binary Tree. 
            var tree = BinaryTree<short>.Create(_state.DecodingTable);

            int dictSize = symbolCodeList.Count;
            if (dictSize >= short.MaxValue)
                throw new NotSupportedException($"We do not support dictionaries with more items than {short.MaxValue - 1}");

            // We haven't stored it yet. So we are calculating it. 
            int numberOfEntriesInTable = (_state.EncodingTable.Length - 4) / Unsafe.SizeOf<Interval3Gram>();
            if (numberOfEntriesInTable < dictSize)
            {
                if (_state.CanGrow)
                    _state.Grow(dictSize);
                else
                    throw new InsufficientMemoryException("Not enough memory for the table and the table supplied does not support growing.");
            }                

            int maxBitSequenceLength = 1;
            int minBitSequenceLength = int.MaxValue;

            for (int i = 0; i < dictSize; i++)
            {
                var symbol = symbolCodeList[i];
                int symbolLength = symbol.Length;

                ref var entry = ref EncodingTable[i];

                // We update the size first to avoid getting a zero size start key.
                entry.KeyLength = (byte)symbolLength;
                for (int j = 0; j < 3; j++)
                {
                    if (j < symbolLength)
                        entry.KeyBuffer[j] = symbol.StartKey[j];
                    else
                        entry.KeyBuffer[j] = 0;
                }

                if (i < dictSize - 1)
                {
                    entry.PrefixLength = 0;

                    // We want a local copy of it. 
                    var nextSymbol = symbolCodeList[i + 1];
                    int nextSymbolLength = nextSymbol.Length;

                    nextSymbol.StartKey[nextSymbolLength - 1] -= 1;
                    int j = 0;
                    while (j < symbolLength && j < nextSymbolLength && symbol.StartKey[j] == nextSymbol.StartKey[j])
                    {
                        entry.PrefixLength++;
                        j++;
                    }
                }
                else
                {
                    entry.PrefixLength = (byte)symbolLength;
                }

                Debug.Assert(entry.PrefixLength > 0);

                entry.Code = symbolCodeList[i].Code;

                uint codeValue = (uint)entry.Code.Value << (sizeof(int) * 8 - entry.Code.Length);
                
                maxBitSequenceLength = Math.Max(maxBitSequenceLength, entry.Code.Length);
                minBitSequenceLength = Math.Min(minBitSequenceLength, entry.Code.Length);

                tree.Add(codeValue, entry.Code.Length, (short)i);
            }

            _entries = dictSize;
            NumberOfEntries = dictSize;
            MaxBitSequenceLength = maxBitSequenceLength;
            MinBitSequenceLength = minBitSequenceLength;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static int Lookup(uint symbolValue, Interval3Gram* table, int numberOfEntries, out Code code)
        {
            // PERF: this is an optimized version of the CompareDictionaryEntry routine. Given that we
            // can actually perform the operation in parallel. The usual case will be to call the parallel
            // version instead of the serial, until we hit the end of the key. 

            uint bot = 0;
            uint top = (uint)numberOfEntries;

            while (top > 1)
            {
                uint mid = top / 2;
                uint codeValue = BinaryPrimitives.ReverseEndianness(table[bot + mid].BufferAndLength) >> 8;

                if (Sse.IsSupported)
                    Sse.Prefetch1(table + bot + mid);

                if (symbolValue >= codeValue)
                    bot += mid;
                top -= mid;
            }

            code = table[bot].Code;
            return table[bot].PrefixLength;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private int Lookup(in BitReader reader, ref Span<byte> symbol, Interval3Gram* table, in BinaryTree<short> tree, out bool endsWithNull)
        {
            BitReader localReader = reader;
            if (tree.FindCommonPrefix(ref localReader, out var idx))
            {
                var p = table + idx;

                int prefixLength = p->PrefixLength;
                byte* buffer = p->KeyBuffer;

                if (prefixLength == 1)
                {
                    symbol[0] = buffer[0];
                }
                else if (prefixLength == 2)
                {
                    symbol[0] = buffer[0];
                    symbol[1] = buffer[1];
                }
                else
                {
                    Span<byte> term = new(p->KeyBuffer, p->PrefixLength);
                    term.CopyTo(symbol);
                }

                endsWithNull = buffer[prefixLength - 1] == 0;

                symbol = symbol[prefixLength..];
                return reader.Length - localReader.Length;
            }

            endsWithNull = false;
            symbol = Span<byte>.Empty;
            return -1;
        }

        public void Dispose()
        {
            _state.Dispose();
        }
    }
}
