using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Sparrow.Binary;
using Sparrow.Collections;

namespace Sparrow.Server.Compression
{
    internal unsafe class Encoder3GramSymbolSelector<TSampleEnumerator>
        where TSampleEnumerator : struct, IReadOnlySpanEnumerator
    {
        private struct ByFrequencyComparer : IComparer<SymbolFrequency>
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public int Compare(SymbolFrequency x, SymbolFrequency y)
            {
                if (y.Frequency != x.Frequency)
                    return y.Frequency - x.Frequency;

                return y.StartKey.SequenceCompareTo(x.StartKey);
            }
        }

        private struct LexicographicComparer : IComparer<SymbolFrequency>
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public int Compare(SymbolFrequency x, SymbolFrequency y)
            {
                return x.StartKey.SequenceCompareTo(y.StartKey);
            }
        }

        private readonly FastList<int> _intervalFrequencies = new(32);
        private readonly FastList<Symbol> _intervalPrefixes = new(32);
        private readonly FastList<Symbol> _intervalBoundaries = new(32);
        private readonly FastList<SymbolFrequency> _mostFrequentSymbols = new (32);

        public FastList<SymbolFrequency> SelectSymbols(TSampleEnumerator keys, int dictionarySize, FastList<SymbolFrequency> symbolFrequenciesList = null)
        {
            CountSymbolFrequency(keys);

            int adjustedDictionarySize = dictionarySize;
            if (dictionarySize > _frequencyMap.Count() * 2)
            {
                // 3 Gram: Input dictionary Size is too big, changing the size
                adjustedDictionarySize = (_frequencyMap.Count() * 2 - 1) / 2;
            }

            PickMostFreqSymbols(adjustedDictionarySize, _mostFrequentSymbols, _frequencyMap);

            FillInGap(_mostFrequentSymbols, _intervalPrefixes, _intervalBoundaries);
            Debug.Assert(_intervalPrefixes.Count == _intervalBoundaries.Count);
            
            CountIntervalFreq(keys, _intervalFrequencies, _intervalPrefixes, _intervalBoundaries);
            Debug.Assert(_intervalPrefixes.Count == _intervalFrequencies.Count);


            if (symbolFrequenciesList == null)
                symbolFrequenciesList = new FastList<SymbolFrequency>();
            else
                symbolFrequenciesList.Clear();

            for (int i = 0; i < (int)_intervalBoundaries.Count; i++)
            {
                symbolFrequenciesList.AddByRef( new SymbolFrequency(_intervalBoundaries[i].StartKey, _intervalFrequencies[i]));
            }

            return symbolFrequenciesList;
        }

        private const int GramSize = 3;
        private readonly Dictionary<int, int> _frequencyMap = new (16);

        private void CountIntervalFreq(TSampleEnumerator keys, FastList<int> intervalFrequencies, FastList<Symbol> intervalPrefixes, FastList<Symbol> intervalBoundaries)
        {
            intervalFrequencies.Clear();
            for (int i = 0; i < intervalPrefixes.Count; i++)
                intervalFrequencies.Add(1);

            keys.Reset();
            while (keys.MoveNext(out var key))
            {
                int pos = 0;

                ref var currentKeyStart = ref MemoryMarshal.GetReference(key);
                while (pos < key.Length)
                {
                    int keyAsInteger;

                    ref var currentKey = ref Unsafe.AddByteOffset(ref currentKeyStart, pos);
                    switch (key.Length - pos)
                    {
                        case 1:
                            keyAsInteger = (currentKey << 16);
                            break;
                        case 2:
                            keyAsInteger = (currentKey << 16 | Unsafe.AddByteOffset(ref currentKey, 1) << 8);
                            break;
                        default:
                            keyAsInteger = (currentKey << 16 | Unsafe.AddByteOffset(ref currentKey, 1) << 8 | Unsafe.AddByteOffset(ref currentKey, 2));
                            break;
                    }

                    int idx = BinarySearch(keyAsInteger, intervalBoundaries);
                    intervalFrequencies[idx]++;
                    pos += intervalPrefixes[idx].Length;
                }
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private int BinarySearch(int key, FastList<Symbol> intervalBoundaries)
        {
            int l = 0;
            int r = intervalBoundaries.Count;
            while (r - l > 1)
            {
                int m = (l + r) >> 1;

                var mKey = intervalBoundaries[m].StartKeyAsInt >> 8;

                int cmp = (int)(key - mKey);
                if (cmp < 0)
                {
                    r = m;
                }
                else if (cmp == 0)
                {
                    return m;
                }
                else
                {
                    l = m;
                }
            }
            return l;
        }

        private void FillInGap(FastList<SymbolFrequency> mostFrequentSymbols, FastList<Symbol> intervalPrefixes, FastList<Symbol> intervalBoundaries)
        {
            intervalPrefixes.Clear();
            intervalBoundaries.Clear();

            if (mostFrequentSymbols.Count == 0)
            {
                FillInSingleChar(0, 255, intervalPrefixes, intervalBoundaries);
                return;
            }

            // Include prefixes and boundaries for every case until we hit the most frequent start key first character. 
            FillInSingleChar(0, mostFrequentSymbols[0].StartKey[0], intervalPrefixes, intervalBoundaries);

            Span<byte> auxiliaryKey = stackalloc byte[GramSize];

            for (int i = 0; i < mostFrequentSymbols.Count - 1; i++)
            {
                var localAuxiliaryKey = auxiliaryKey;

                var key1 = new Symbol(mostFrequentSymbols[i].StartKey);
                var key2 = new Symbol(mostFrequentSymbols[i + 1].StartKey);

                intervalPrefixes.AddByRef(key1);
                intervalBoundaries.AddByRef(key1);

                var key1RightBound = key1;
                for (int j = 3 - 1; j >= 0; j--)
                {
                    if (key1RightBound.StartKey[j] < 255)
                    {
                        key1RightBound.StartKey[j] += 1;
                        key1RightBound = new Symbol(key1RightBound.StartKey.Slice(0, j + 1));
                        break;
                    }
                }

                if (key1RightBound.StartKey.SequenceCompareTo(key2.StartKey) != 0)
                {
                    intervalBoundaries.AddByRef(new Symbol(key1RightBound.StartKey));
                    if (key1RightBound.StartKey[0] != key2.StartKey[0])
                    {
                        intervalPrefixes.AddByRef(new Symbol(key1.StartKey.Slice(0, 1)));
                        FillInSingleChar(key1.StartKey[0] + 1, key2.StartKey[0], intervalPrefixes, intervalBoundaries);
                    }
                    else
                    {
                        int length;
                        if (key1.StartKey[0] != key1RightBound.StartKey[0])
                        {
                            localAuxiliaryKey = key1RightBound.StartKey;
                            length = localAuxiliaryKey.Length;
                        }
                        else
                        {
                            length = CommonPrefix(localAuxiliaryKey, key1.StartKey, key2.StartKey);
                        }

                        Debug.Assert(length > 0);
                        intervalPrefixes.AddByRef(new Symbol(localAuxiliaryKey.Slice(0, length)));
                    }
                }
            }

            var lastKey = new Symbol(mostFrequentSymbols[^1].StartKey);
            intervalPrefixes.Add(lastKey);
            intervalBoundaries.Add(lastKey);

            var lastKeyRightBound = lastKey;
            for (int j = 3 - 1; j >= 0; j--)
            {
                if (lastKeyRightBound.StartKey[j] < 255)
                {
                    lastKeyRightBound.StartKey[j] += 1;
                    lastKeyRightBound = new Symbol(lastKeyRightBound.StartKey.Slice(0, j + 1));
                    break;
                }
            }

            intervalBoundaries.AddByRef(new Symbol(lastKeyRightBound.StartKey));
            intervalPrefixes.AddByRef(new Symbol(lastKeyRightBound.StartKey.Slice(0, 1)));

            if (lastKey.StartKey[0] < 255)
                FillInSingleChar(lastKey.StartKey[0] + 1, 255, intervalPrefixes, intervalBoundaries);
        }

        private int CommonPrefix(Span<byte> commonStr, ReadOnlySpan<byte> str1, ReadOnlySpan<byte> str2)
        {
            if (str1[0] != str2[0])
                return 0; 

            for (int i = 1; i < GramSize; i++)
            {
                if (str1.Length < i)
                {
                    str1.Slice(0, i).CopyTo(commonStr);
                    return i;
                }

                if (str2.Length < i)
                {
                    str2.Slice(0, i).CopyTo(commonStr);
                    return i;
                }

                if (str1[i] != str2[i])
                {
                    str1.Slice(0, i).CopyTo(commonStr);
                    return i;
                }
            }

            throw new ArgumentException();
        }

        private void FillInSingleChar(int start, int last, FastList<Symbol> intervalPrefixes, FastList<Symbol> intervalBoundaries)
        {
            Span<byte> key = stackalloc byte[1];
            for (int c = start; c <= last; c++)
            {
                // We update the local key to be pushed to the prefixes and boundaries.
                key[0] = (byte)c;

                // Create a symbol out of the single character and push it into the prefixes and boundaries list.
                var singleCharacterSymbol = new Symbol(key);
                intervalPrefixes.AddByRef(singleCharacterSymbol);
                intervalBoundaries.AddByRef(singleCharacterSymbol);
            }
        }

        private void PickMostFreqSymbols(int dictionarySize, FastList<SymbolFrequency> mostFrequentSymbols, Dictionary<int, int> frequencyMap)
        {
            Debug.Assert(GramSize == 3);

            mostFrequentSymbols.Clear();

            Span<byte> key = stackalloc byte[4];
            Span<byte> keySlice = key.Slice(0, 3);
            foreach (var tuple in frequencyMap)
            {
                key[0] = (byte)(tuple.Key >> 16);
                key[1] = (byte)(tuple.Key >> 8);
                key[2] = (byte)(tuple.Key);

                mostFrequentSymbols.AddByRef(new SymbolFrequency(keySlice, tuple.Value));
            }

            Sorter<SymbolFrequency, ByFrequencyComparer> sortByFrequency;
            mostFrequentSymbols.Sort(ref sortByFrequency);

            if (mostFrequentSymbols.Count > dictionarySize)
                mostFrequentSymbols.Trim(dictionarySize);

            Sorter<SymbolFrequency, LexicographicComparer> sortLexicographical;
            mostFrequentSymbols.Sort(ref sortLexicographical);
        }

        private void CountSymbolFrequency(TSampleEnumerator keys)
        {
            Debug.Assert(GramSize <= 3);

            // PERF: Local reference to avoid having to access the field every time.
            var frequencyMap = _frequencyMap;

            frequencyMap.Clear();

            keys.Reset();
            while (keys.MoveNext(out var key))
            {
                for (int j = 0; j < key.Length - GramSize + 1; j++)
                {
                    var slice = key.Slice(j, GramSize);
                    Debug.Assert(slice.Length == GramSize);

                    // To support nulls, we need to ensure that we are not going to be counting any symbol that has nulls 
                    // in it's content to force the frequency of such an even to go to 0. 
                    // https://issues.hibernatingrhinos.com/issue/RavenDB-19703
                    if (slice[0] == 0 || slice[1] == 0 || slice[2] == 0)
                        continue;

                    int sliceDescriptor = slice[0] << 16 | slice[1] << 8 | slice[2];
                    ref var freq = ref CollectionsMarshal.GetValueRefOrAddDefault(frequencyMap, sliceDescriptor, out _);
                    freq++;
                }
            }          
        }
    }
}
