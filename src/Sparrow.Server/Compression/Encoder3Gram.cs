using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Numerics;
using System.Reflection.Metadata.Ecma335;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics.X86;
using System.Security.Cryptography;
using Sparrow.Collections;
using Sparrow.Server.Binary;
using Sparrow.Server.Collections.Persistent;

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
        private TEncoderState _state;
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
            ref var tablePtr = ref EncodingTable[0];

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

                        int prefixLen = Lookup(symbolValue, ref tablePtr, numberOfEntries, out Code code);

                        // PERF: While naturally this would happen at the end of the loop, as they are not used anymore
                        // we can do the work here instead and allow the JIT to reschedule the registers to be used on
                        // the next block and fore-go on keep tracking of them. 
                        keyLength -= prefixLen;
                        keyPtr += prefixLen;

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
                    }

                    intBuf[idx] = BinaryPrimitives.ReverseEndianness(bitWriterBuffer << (64 - intBufLen));
                    outputSizes[i] = ((idx << 6) + intBufLen);
                }
            }
            
        }

        public void DecodeBatch<TSampleEnumerator, TOutputEnumerator>(in TSampleEnumerator data, Span<int> outputSize, in TOutputEnumerator outputBuffers)
            where TSampleEnumerator : struct, IReadOnlySpanIndexer
            where TOutputEnumerator : struct, ISpanIndexer
        {
            fixed (Interval3Gram* table = EncodingTable)
            {
                var tree = BinaryTree<short>.Open(_state.DecodingTable);

                for (int i = 0; i < data.Length; i++)
                {
                    Span<byte> buffer =  outputBuffers[i];
                    var reader = new BitReader(data[i]);
                    int bits = reader.Length;
                    var endsWithNull = false; 
                    while (bits > 0 && endsWithNull == false)
                    {
                        int length = Lookup(reader, ref buffer, table, tree, out endsWithNull);
                        if (length < 0)
                            throw new IOException("Invalid data stream.");
                        // Advance the reader.
                        reader.Skip(length);
                        bits -= length;
                    }

                    outputSize[i] = buffer.Length - buffer.Length;
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


        public void DecodeBatch<TSampleEnumerator, TOutputEnumerator>(ReadOnlySpan<int> dataBits, in TSampleEnumerator data, Span<int> outputSize, in TOutputEnumerator outputBuffers)
            where TSampleEnumerator : struct, IReadOnlySpanIndexer
            where TOutputEnumerator : struct, ISpanIndexer
        {
            fixed (Interval3Gram* table = EncodingTable)
            {
                var tree = BinaryTree<short>.Open(_state.DecodingTable);

                for (int i = 0; i < data.Length; i++)
                {
                    Span<byte> buffer = default;
                    var reader = new BitReader(data[i]);
                    int bits = dataBits[i];
                    var endsWithNull = false;
                    while (bits > 0 && endsWithNull == false)
                    {
                        buffer = outputBuffers[i];
                        int length = Lookup(reader, ref buffer, table, tree, out endsWithNull);
                        if (length < 0)
                            throw new IOException("Invalid data stream.");

                        // Advance the reader.
                        reader.Skip(length);

                        bits -= length;
                    }

                    outputSize[i] = buffer.Length - buffer.Length;
                }
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
        public int Encode(ReadOnlySpan<byte> key, Span<byte> outputBuffer)
        {
            Debug.Assert(outputBuffer.Length >= sizeof(long)); // Ensure we can safely cast to int 64

            ref var tablePtr = ref GetEncodingTablePtr();
            ref var keyStartPtr = ref MemoryMarshal.GetReference(key);
            ref long intBufPtr = ref MemoryMarshal.Cast<byte, long>(outputBuffer)[0];
            ref long intBufStartPtr = ref intBufPtr;

            intBufPtr = 0;

            var numberOfEntries = _entries;

            int keyPtr = 0;
            var keyLength = key.Length;
            
            int intBufLen = 0;
            long bitWriterBuffer = 0;
            while (keyLength > 0)
            {
                ref var currentKeyPtr = ref Unsafe.AddByteOffset(ref keyStartPtr, new IntPtr(keyPtr));

                // Initialize the important variables.
                uint symbolValue;
                if (keyLength >= 4)
                {
                    // PERF: This is the usual case, this is the branch that gets executed for most of the
                    // key, at least until we hit the tail.
                    
                    symbolValue = BinaryPrimitives.ReverseEndianness(Unsafe.ReadUnaligned<uint>(ref currentKeyPtr)) >> 8;
                }
                else
                {
                    // PERF: we are performing masked copy with reverse endianness using tables, with
                    // the objective of diminishing the amount of instructions necessary for execution.
                    // The switch implementation requires over 40 instructions including several branches,
                    // this version is much more succinct and do not need any jump. 

                    // We copy to the symbols buffer the part of the key we will be consuming.
                    int tableIndex = keyLength * 4;

                    symbolValue = (uint)(Unsafe.ReadUnaligned<byte>(ref Unsafe.AddByteOffset(ref currentKeyPtr, EncodingTailTable[tableIndex])) << 16 |
                                         Unsafe.ReadUnaligned<byte>(ref Unsafe.AddByteOffset(ref currentKeyPtr, EncodingTailTable[tableIndex + 1])) << 8 |
                                         Unsafe.ReadUnaligned<byte>(ref Unsafe.AddByteOffset(ref currentKeyPtr, EncodingTailTable[tableIndex + 2])));

                    symbolValue &= 0xFFFFFFFF << EncodingTailMaskTable[keyLength];
                }

                int prefixLen = Lookup(symbolValue, ref tablePtr, numberOfEntries, out Code code);

                // PERF: While naturally this would happen at the end of the loop, as they are not used anymore
                // we can do the work here instead and allow the JIT to reschedule the registers to be used on
                // the next block and fore-go on keep tracking of them. 
                keyLength -= prefixLen;
                keyPtr += prefixLen;

                // Perform the actual encoding in a bitwise fashion. 
                long sBuf = code.Value;
                int sLen = code.Length;
                if (intBufLen + sLen > 63)
                {
                    int numBitsLeft = 64 - intBufLen;
                    intBufLen = sLen - numBitsLeft;

                    intBufPtr = BinaryPrimitives.ReverseEndianness((bitWriterBuffer << numBitsLeft) | (sBuf >> intBufLen));
                    intBufPtr = ref Unsafe.Add(ref intBufPtr, 1);
                    bitWriterBuffer = sBuf;
                }
                else
                {
                    bitWriterBuffer = (bitWriterBuffer << sLen) | sBuf;
                    intBufLen += sLen;
                }
            }

            intBufPtr = BinaryPrimitives.ReverseEndianness(bitWriterBuffer << (64 - intBufLen));

            var idx = Unsafe.ByteOffset(ref intBufStartPtr, ref intBufPtr).ToInt64() / sizeof(long);
            return (int)(idx << 6) + intBufLen;
            
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Decode(ReadOnlySpan<byte> data, Span<byte> outputBuffer)
        {
            return Decode(data.Length * 8, data, outputBuffer);
        }

        [SkipLocalsInit]
        public int Decode(int bits, ReadOnlySpan<byte> data, Span<byte> outputBuffer)
        {
            ref Interval3Gram tableRef = ref EncodingTable[0];
            ref byte symbolsPtrRef = ref outputBuffer[0];

            var tree = BinaryTree<short>.Open(_state.DecodingTable);
            ref byte dataRef = ref MemoryMarshal.GetReference(data);

            int bitStreamLength = bits;
            int readerBitIdx = 0;

            byte* auxBuffer = stackalloc byte[4];
            nuint offset = 0;
            while (readerBitIdx < bitStreamLength)
            {
                readerBitIdx = tree.FindCommonPrefix(ref dataRef, bitStreamLength, readerBitIdx, out var idx);
                if (readerBitIdx == -1)
                    goto Fail;

                var p = Unsafe.AddByteOffset(ref tableRef, (IntPtr) (idx * Unsafe.SizeOf<Interval3Gram>()));

                int prefixLength = p.PrefixLength;
                *(uint*)auxBuffer = p.BufferAndLength;

                if (prefixLength == 1)
                {
                    Unsafe.WriteUnaligned(ref Unsafe.AddByteOffset(ref symbolsPtrRef, offset), auxBuffer[0]);
                }
                else if (prefixLength == 2)
                {
                    Unsafe.WriteUnaligned(ref Unsafe.AddByteOffset(ref symbolsPtrRef, offset), auxBuffer[0]);
                    Unsafe.WriteUnaligned(ref Unsafe.AddByteOffset(ref symbolsPtrRef, offset+1), auxBuffer[1]);
                }
                else
                {
                    Unsafe.WriteUnaligned(ref Unsafe.AddByteOffset(ref symbolsPtrRef, offset), auxBuffer[0]);
                    Unsafe.WriteUnaligned(ref Unsafe.AddByteOffset(ref symbolsPtrRef, offset+1), auxBuffer[1]);
                    Unsafe.WriteUnaligned(ref Unsafe.AddByteOffset(ref symbolsPtrRef, offset+2), auxBuffer[2]);
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
            set { MemoryMarshal.Write(_state.EncodingTable, in value);}
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
                MemoryMarshal.Write(_state.EncodingTable.Slice(4, 1), in valueAsByte);
            }
        }

        public int MinBitSequenceLength
        {
            get { return MemoryMarshal.Read<byte>(_state.EncodingTable.Slice(5, 1)); }
            set
            {
                byte valueAsByte = (byte)value;
                MemoryMarshal.Write(_state.EncodingTable.Slice(5, 1), in valueAsByte);
            }
        }

        private Span<Interval3Gram> EncodingTable => MemoryMarshal.Cast<byte, Interval3Gram>(_state.EncodingTable.Slice(8));

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private ref Interval3Gram GetEncodingTablePtr()
        {
            ref byte ptr = ref Unsafe.Add(ref MemoryMarshal.GetReference(_state.EncodingTable), 8);
            return ref Unsafe.As<byte, Interval3Gram>(ref ptr);
        }

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

                if (entry.PrefixLength == 0)
                {
                    i--;
                    continue;
                }
                    
                Debug.Assert(entry.PrefixLength > 0);

                entry.Code = symbolCodeList[i].Code;

                maxBitSequenceLength = Math.Max(maxBitSequenceLength, entry.Code.Length);
                minBitSequenceLength = Math.Min(minBitSequenceLength, entry.Code.Length);

                tree.Add((uint)entry.Code.Value, entry.Code.Length, (short)i);
            }

            _entries = dictSize;
            NumberOfEntries = dictSize;
            MaxBitSequenceLength = maxBitSequenceLength;
            MinBitSequenceLength = minBitSequenceLength;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static int Lookup(uint symbolValue, ref Interval3Gram tablePtr, int numberOfEntries, out Code code)
        {
            // PERF: this is an optimized version of the CompareDictionaryEntry routine. Given that we
            // can actually perform the operation in parallel. The usual case will be to call the parallel
            // version instead of the serial, until we hit the end of the key. 

            uint bot = 0;
            uint top = (uint)numberOfEntries;
            
            while (top > 1)
            {
                uint mid = top / 2;

                // PERF: This is table[bot+mid].
                ref var tableItem = ref Unsafe.Add(ref tablePtr, bot + mid);
                uint codeValue = BinaryPrimitives.ReverseEndianness(tableItem.BufferAndLength) >> 8;

                if (symbolValue >= codeValue)
                    bot += mid;
                top -= mid;
            }

            // To guarantee that encoding always makes progress, we must ensure that every dictionary lookup is successful.
            // https://db.cs.cmu.edu/papers/2020/zhang-sigmod2020.pdf [Page 1603]
            ref var botItem = ref Unsafe.Add(ref tablePtr, bot);
            code = botItem.Code;
            return botItem.PrefixLength;
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
