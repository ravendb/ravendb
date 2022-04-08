using System;
using System.Buffers.Binary;
using System.Diagnostics;
using System.IO;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Sparrow.Collections;
using Sparrow.Server.Binary;
using Sparrow.Server.Collections.Persistent;

namespace Sparrow.Server.Compression
{
    [StructLayout(LayoutKind.Explicit)]
    internal unsafe struct Interval3Gram
    {
        [FieldOffset(0)]
        private byte _startKey;
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

        public Span<byte> StartKey => new(Unsafe.AsPointer(ref _startKey), 3);
    }

    public struct Encoder3Gram<TEncoderState> : IEncoderAlgorithm
        where TEncoderState : struct, IEncoderState
    {
        private readonly TEncoderState _state;
        private int _entries;

        public Encoder3Gram(TEncoderState state)
        {
            _state = state;
            _entries = 0; // Zero means not trained.

            _entries = _numberOfEntries[0];
        }

        public static int GetDictionarySize(in TEncoderState state)
        {
            int entries = MemoryMarshal.Cast<byte, int>(state.EncodingTable.Slice(0, 4))[0];
            return entries * Unsafe.SizeOf<Interval3Gram>() + 8;
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
            where TSampleEnumerator : struct, IReadOnlySpanEnumerator
            where TOutputEnumerator : struct, ISpanEnumerator
        {
            var table = EncodingTable;
            var numberOfEntries = _entries;

            int length = data.Length;
            for (int i = 0; i < length; i++)
            {
                var intBuf = MemoryMarshal.Cast<byte, long>(outputBuffers[i]);

                int idx = 0;
                intBuf[0] = 0;
                int intBufLen = 0;

                var keyStr = data[i];
                int pos = 0;
                while (pos < keyStr.Length)
                {
                    int prefixLen = Lookup(keyStr.Slice(pos), table, numberOfEntries, out Code code);
                    long sBuf = code.Value;
                    int sLen = code.Length;
                    if (intBufLen + sLen > 63)
                    {
                        int numBitsLeft = 64 - intBufLen;
                        intBufLen = sLen - numBitsLeft;
                        intBuf[idx] <<= numBitsLeft;
                        intBuf[idx] |= (sBuf >> intBufLen);
                        intBuf[idx] = BinaryPrimitives.ReverseEndianness(intBuf[idx]);
                        intBuf[idx + 1] = sBuf;
                        idx++;
                    }
                    else
                    {
                        intBuf[idx] <<= sLen;
                        intBuf[idx] |= sBuf;
                        intBufLen += sLen;
                    }

                    pos += prefixLen;
                }

                intBuf[idx] <<= (64 - intBufLen);
                intBuf[idx] = BinaryPrimitives.ReverseEndianness(intBuf[idx]);
                outputSizes[i] = ((idx << 6) + intBufLen);
            }
        }

        public void DecodeBatch<TSampleEnumerator, TOutputEnumerator>(in TSampleEnumerator data, Span<int> outputSize, in TOutputEnumerator outputBuffers)
            where TSampleEnumerator : struct, IReadOnlySpanEnumerator
            where TOutputEnumerator : struct, ISpanEnumerator
        {
            var table = EncodingTable;
            var tree = BinaryTree<short>.Open(_state.DecodingTable);

            for (int i = 0; i < data.Length; i++)
            {
                var buffer = outputBuffers[i];
                var reader = new BitReader(data[i]);
                int bits = reader.Length;
                while (bits > 0)
                {
                    int length = Lookup(reader, out var symbol, table, tree);
                    if (length < 0)
                        throw new IOException("Invalid data stream.");

                    // Advance the reader.
                    reader.Skip(length);

                    symbol.CopyTo(buffer);
                    buffer = buffer.Slice(symbol.Length);

                    bits -= length;

                    if (symbol[^1] == 0)
                        break;
                }

                outputSize[i] = buffer.Length - buffer.Length;
            }
        }

        public void DecodeBatch<TSampleEnumerator, TOutputEnumerator>(ReadOnlySpan<int> dataBits, in TSampleEnumerator data, Span<int> outputSize, in TOutputEnumerator outputBuffers)
            where TSampleEnumerator : struct, IReadOnlySpanEnumerator
            where TOutputEnumerator : struct, ISpanEnumerator
        {
            var table = EncodingTable;
            var tree = BinaryTree<short>.Open(_state.DecodingTable);

            for (int i = 0; i < data.Length; i++)
            {
                var buffer = outputBuffers[i];
                var reader = new BitReader(data[i]);
                int bits = dataBits[i];
                while (bits > 0)
                {
                    int length = Lookup(reader, out var symbol, table, tree);
                    if (length < 0)
                        throw new IOException("Invalid data stream.");

                    // Advance the reader.
                    reader.Skip(length);

                    symbol.CopyTo(buffer);
                    buffer = buffer.Slice(symbol.Length);

                    bits -= length;
                }

                outputSize[i] = buffer.Length - buffer.Length;
            }
        }

        public int Encode(ReadOnlySpan<byte> key, Span<byte> outputBuffer)
        {
            Debug.Assert(outputBuffer.Length >= sizeof(long)); // Ensure we can safely cast to int 64

            var intBuf = MemoryMarshal.Cast<byte, long>(outputBuffer);

            int idx = 0;
            intBuf[0] = 0;
            int intBufLen = 0;

            var table = EncodingTable;
            var numberOfEntries = _entries;

            var symbol = key;
            while (symbol.Length != 0)
            {
                int prefixLen = Lookup(symbol, table, numberOfEntries, out Code code);
                long sBuf = code.Value;
                int sLen = code.Length;
                if (intBufLen + sLen > 63)
                {
                    int numBitsLeft = 64 - intBufLen;
                    intBufLen = sLen - numBitsLeft;
                    intBuf[idx] <<= numBitsLeft;
                    intBuf[idx] |= (sBuf >> intBufLen);
                    intBuf[idx] = BinaryPrimitives.ReverseEndianness(intBuf[idx]);
                    intBuf[idx + 1] = sBuf;
                    idx++;
                }
                else
                {
                    intBuf[idx] <<= sLen;
                    intBuf[idx] |= sBuf;
                    intBufLen += sLen;
                }

                symbol = symbol.Slice(prefixLen);
            }

            intBuf[idx] <<= (64 - intBufLen);
            intBuf[idx] = BinaryPrimitives.ReverseEndianness(intBuf[idx]);
            return ((idx << 6) + intBufLen);
        }

        public int DecodeStochasticBug(ReadOnlySpan<byte> data, Span<byte> outputBuffer)
        {
            var buffer = outputBuffer;
            var table = EncodingTable;
            var tree = BinaryTree<short>.Open(_state.DecodingTable);

            var reader = new BitReader(data);
            while (reader.Length > 0)
            {
                int length = Lookup(reader, out var symbol, table, tree);
                if (length < 0)
                    throw new IOException("Invalid data stream.");

                // Need to check here because the compiler does something strange after the Skip() call and kills
                // the memory content of the symbol ReadOnlySpan.
                bool hasFinished = symbol[^1] == 0;
                symbol.CopyTo(buffer);
                buffer = buffer.Slice(symbol.Length);

                // Advance the reader.
                reader.Skip(length);

                if (hasFinished)
                    break;
            }

            return outputBuffer.Length - buffer.Length;
        }

        public int Decode(ReadOnlySpan<byte> data, Span<byte> outputBuffer)
        {
            var buffer = outputBuffer;
            var table = EncodingTable;
            var tree = BinaryTree<short>.Open(_state.DecodingTable);

            var reader = new BitReader(data);
            while (reader.Length > 0)
            {
                int length = Lookup(reader, out var symbol, table, tree);
                if (length < 0)
                    throw new IOException("Invalid data stream.");

                symbol.CopyTo(buffer);
                buffer = buffer.Slice(symbol.Length);

                // Advance the reader.
                reader.Skip(length);

                if (symbol[^1] == 0)
                    break;
            }

            return outputBuffer.Length - buffer.Length;
        }

        public int Decode(int bits, ReadOnlySpan<byte> data, Span<byte> outputBuffer)
        {
            var table = EncodingTable;
            var tree = BinaryTree<short>.Open(_state.DecodingTable);
            var buffer = outputBuffer;
            var reader = new BitReader(data);
            while (bits > 0)
            {
                int length = Lookup(reader, out var symbol, table, tree);
                if (length < 0)
                    throw new IOException("Invalid data stream.");

                // Advance the reader.
                reader.Skip(length);

                symbol.CopyTo(buffer);
                buffer = buffer.Slice(symbol.Length);

                bits -= length;
            }

            return outputBuffer.Length - buffer.Length;
        }

        public int NumberOfEntries => _numberOfEntries[0];
        public int MemoryUse => _numberOfEntries[0] * Unsafe.SizeOf<Interval3Gram>();


        private Span<int> _numberOfEntries => MemoryMarshal.Cast<byte, int>(_state.EncodingTable.Slice(0, 4));

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

        private void BuildDictionary(in FastList<SymbolCode> symbolCodeList)
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
                throw new ArgumentException("Not enough memory to store the dictionary");

            int maxBitSequenceLength = 1;
            int minBitSequenceLength = int.MaxValue;

            for (int i = 0; i < dictSize; i++)
            {
                var symbol = symbolCodeList[i];
                int symbolLength = symbol.Length;

                ref var entry = ref EncodingTable[i];

                // We update the size first to avoid getting a zero size start key.
                entry.KeyLength = (byte)symbolLength;
                var startKey = entry.StartKey;

                for (int j = 0; j < 3; j++)
                {
                    if (j < symbolLength)
                        startKey[j] = symbol.StartKey[j];
                    else
                        startKey[j] = 0;
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

                int codeValue = BinaryPrimitives.ReverseEndianness(entry.Code.Value << (sizeof(int) * 8 - entry.Code.Length));
                var codeValueSpan = MemoryMarshal.Cast<int, byte>(MemoryMarshal.CreateSpan(ref codeValue, 1));
                var reader = new BitReader(codeValueSpan, entry.Code.Length);
                maxBitSequenceLength = Math.Max(maxBitSequenceLength, entry.Code.Length);
                minBitSequenceLength = Math.Min(minBitSequenceLength, entry.Code.Length);

                tree.Add(ref reader, (short)i);
            }

            _numberOfEntries[0] = dictSize;
            _entries = dictSize;
            MaxBitSequenceLength = maxBitSequenceLength;
            MinBitSequenceLength = minBitSequenceLength;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private int Lookup(ReadOnlySpan<byte> symbol, ReadOnlySpan<Interval3Gram> table, int numberOfEntries, out Code code)
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            static int CompareDictionaryEntry(ReadOnlySpan<byte> s1, ReadOnlySpan<byte> s2)
            {
                for (int i = 0; i < 3; i++)
                {
                    if (i >= s1.Length)
                    {
                        if (s2[i] == 0)
                            return 0;
                        return -1;
                    }

                    if (s1[i] < s2[i])
                        return -1;
                    if (s1[i] > s2[i])
                        return 1;
                }

                if (s1.Length > 3)
                    return 1;
                return 0;
            }

            int l = 0;
            int r = numberOfEntries;
            while (r - l > 1)
            {
                int m = (l + r) >> 1;

                int cmp = CompareDictionaryEntry(symbol, table[m].StartKey);
                if (cmp < 0)
                {
                    r = m;
                }
                else if (cmp == 0)
                {
                    l = m;
                    break;
                }
                else
                {
                    l = m;
                }
            }

            code = table[l].Code;
            return table[l].PrefixLength;
        }

        private int Lookup(in BitReader reader, out ReadOnlySpan<byte> symbol, ReadOnlySpan<Interval3Gram> table, in BinaryTree<short> tree)
        {
            BitReader localReader = reader;
            if (tree.FindCommonPrefix(ref localReader, out var idx))
            {
                // JIT: The runtime decides to create a copy of table[idx] therefore it will create a span on the stack
                //      then the Unsafe at StartKey will get you a dereference to the wrong location and stack spills will
                //      cause errors when decoding. 
                ref var intervalGram = ref Unsafe.AsRef(table[idx]);
                symbol = intervalGram.StartKey.Slice(0, intervalGram.PrefixLength);

                return reader.Length - localReader.Length;
            }

            symbol = ReadOnlySpan<byte>.Empty;
            return -1;
        }
    }
}
