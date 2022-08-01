using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Corax.Utils.Spatial;
using Sparrow;
using static Corax.Queries.SortingMatch;

namespace Corax.Queries
{
    [DebuggerDisplay("{DebugView,nq}")]
    public unsafe struct SortingMultiMatch<TInner, TComparer1, TComparer2, TComparer3, TComparer4, TComparer5, TComparer6, TComparer7, TComparer8, TComparer9> : IQueryMatch
        where TInner : IQueryMatch
        where TComparer1 : IMatchComparer
        where TComparer2 : IMatchComparer
        where TComparer3 : IMatchComparer
        where TComparer4 : IMatchComparer
        where TComparer5 : IMatchComparer
        where TComparer6 : IMatchComparer
        where TComparer7 : IMatchComparer
        where TComparer8 : IMatchComparer
        where TComparer9 : IMatchComparer
    {
        private readonly IndexSearcher _searcher;
        private readonly IQueryMatch _inner;
        private readonly TComparer1 _comparer1;
        private readonly TComparer2 _comparer2;
        private readonly TComparer3 _comparer3;
        private readonly TComparer4 _comparer4;
        private readonly TComparer5 _comparer5;
        private readonly TComparer6 _comparer6;
        private readonly TComparer7 _comparer7;
        private readonly TComparer8 _comparer8;
        private readonly TComparer9 _comparer9;
        private readonly int _totalComparers;
        private readonly int _take;

        public bool HasBoostingComparer => typeof(TComparer1) == typeof(BoostingComparer) || typeof(TComparer2) == typeof(BoostingComparer) || typeof(TComparer3) == typeof(BoostingComparer) ||
                                           typeof(TComparer4) == typeof(BoostingComparer) || typeof(TComparer5) == typeof(BoostingComparer) || typeof(TComparer6) == typeof(BoostingComparer) ||
                                           typeof(TComparer7) == typeof(BoostingComparer) || typeof(TComparer8) == typeof(BoostingComparer) || typeof(TComparer9) == typeof(BoostingComparer);

        private readonly delegate*<ref SortingMultiMatch<TInner, TComparer1, TComparer2, TComparer3, TComparer4, TComparer5, TComparer6, TComparer7, TComparer8, TComparer9>, int, UnmanagedSpan, UnmanagedSpan, float, float, int>[] _compareFuncs;        

        public long TotalResults;

        public long Count => _inner.Count;

        public QueryCountConfidence Confidence => _inner.Confidence;

        public bool IsBoosting => _inner.IsBoosting || HasBoostingComparer;

        internal SortingMultiMatch(
            IndexSearcher searcher, in TInner inner,
            in TComparer1 comparer1 = default,
            in TComparer2 comparer2 = default,
            in TComparer3 comparer3 = default,
            in TComparer4 comparer4 = default,
            in TComparer5 comparer5 = default,
            in TComparer6 comparer6 = default,
            in TComparer7 comparer7 = default,
            in TComparer8 comparer8 = default,
            in TComparer9 comparer9 = default,
            int take = -1)
        {
            _searcher = searcher;
            _inner = inner;
            _take = take;

            // PERF: We dont want to initialize any if we are not going to be using them. 
            Unsafe.SkipInit(out _comparer2);
            Unsafe.SkipInit(out _comparer3);
            Unsafe.SkipInit(out _comparer4);
            Unsafe.SkipInit(out _comparer5);
            Unsafe.SkipInit(out _comparer6);
            Unsafe.SkipInit(out _comparer7);
            Unsafe.SkipInit(out _comparer8);
            Unsafe.SkipInit(out _comparer9);
            
            _compareFuncs = new delegate*<ref SortingMultiMatch<TInner, TComparer1, TComparer2, TComparer3, TComparer4, TComparer5, TComparer6, TComparer7, TComparer8, TComparer9>, int, UnmanagedSpan, UnmanagedSpan, float, float, int>[9];

            TotalResults = 0;

            _comparer1 = comparer1;
            if (typeof(TComparer2) == typeof(SortingMultiMatch.NullComparer))
                throw new NotSupportedException($"{nameof(SortingMultiMatch)} must have at least 2 different comparers. When a single comparer is needed use {nameof(SortingMatch)} instead.");

            _comparer2 = comparer2;
            _compareFuncs[0] = GetFunctionCall<TComparer2>(comparer2.FieldType);
            _totalComparers = 1;

            if (typeof(TComparer3) == typeof(SortingMultiMatch.NullComparer))
                return;
            _comparer3 = comparer3;
            _compareFuncs[1] = GetFunctionCall<TComparer3>(comparer3.FieldType);
            _totalComparers++;

            if (typeof(TComparer4) == typeof(SortingMultiMatch.NullComparer))
                return;
            _comparer4 = comparer4;
            _compareFuncs[2] = GetFunctionCall<TComparer4>(comparer4.FieldType);
            _totalComparers++;

            if (typeof(TComparer5) == typeof(SortingMultiMatch.NullComparer))
                return;
            _comparer5 = comparer5;
            _compareFuncs[3] = GetFunctionCall<TComparer5>(comparer5.FieldType);
            _totalComparers++;

            if (typeof(TComparer6) == typeof(SortingMultiMatch.NullComparer))
                return;
            _comparer6 = comparer6;
            _compareFuncs[4] = GetFunctionCall<TComparer6>(comparer6.FieldType);
            _totalComparers++;

            if (typeof(TComparer7) == typeof(SortingMultiMatch.NullComparer))
                return;
            _comparer7 = comparer7;
            _compareFuncs[5] = GetFunctionCall<TComparer7>(comparer7.FieldType);
            _totalComparers++;

            if (typeof(TComparer8) == typeof(SortingMultiMatch.NullComparer))
                return;
            _comparer8 = comparer8;
            _compareFuncs[6] = GetFunctionCall<TComparer8>(comparer8.FieldType);
            _totalComparers++;

            if (typeof(TComparer9) == typeof(SortingMultiMatch.NullComparer))
                return;
            _comparer9 = comparer9;
            _compareFuncs[7] = GetFunctionCall<TComparer9>(comparer9.FieldType);
            _totalComparers++;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static delegate*<ref SortingMultiMatch<TInner,
            TComparer1, TComparer2, TComparer3, 
            TComparer4, TComparer5, TComparer6, 
            TComparer7, TComparer8, TComparer9>, 
            int, UnmanagedSpan, UnmanagedSpan, float, float, int> GetFunctionCall<TComparer>(MatchCompareFieldType fieldType) where TComparer : IMatchComparer
        {
            return fieldType switch
            {
                MatchCompareFieldType.Sequence => &CompareSequence<TComparer>,
                MatchCompareFieldType.Alphanumeric => &CompareSequence<TComparer>,
                MatchCompareFieldType.Integer => &CompareNumerical<TComparer, long>,
                MatchCompareFieldType.Floating => &CompareNumerical<TComparer, double>,
                MatchCompareFieldType.Score => &CompareNumerical<TComparer, float>,
                MatchCompareFieldType.Spatial => &CompareSpatialRelaxation<TComparer>,
                _ => throw new NotImplementedException(),
            };
        }

        public int AndWith(Span<long> buffer, int matches)
        {
            throw new NotSupportedException($"{nameof(SortingMultiMatch)} does not support the operation {nameof(AndWith)}.");
        }


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Fill(Span<long> matches)
        {
            return _comparer1.FieldType switch
            {
                MatchCompareFieldType.Sequence => Fill<SequenceItem>(matches),
                MatchCompareFieldType.Integer => Fill<NumericalItem<long>>(matches),
                MatchCompareFieldType.Floating => Fill<NumericalItem<double>>(matches),
                MatchCompareFieldType.Score => Fill<NumericalItem<float>>(matches),
                MatchCompareFieldType.Spatial => Fill<NumericalItem<double>>(matches),
                _ => throw new ArgumentOutOfRangeException(_comparer1.FieldType.ToString())
            };
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static bool Get<TOut, TIn>(in IndexEntryReader reader, int fieldId, long x, out TOut storedValue, in TIn comparer)
            where TOut : struct
            where TIn : IMatchComparer
        {
            if (typeof(TIn) == typeof(SpatialAscendingMatchComparer))
            {
                if (comparer is not SpatialAscendingMatchComparer spatialAscendingMatchComparer)
                    goto Failed;

                var readX = reader.Read(fieldId, out (double lat, double lon) coordinates);
                var distance = SpatialUtils.GetGeoDistance(in coordinates, in spatialAscendingMatchComparer);
                
                storedValue = (TOut)(object)new NumericalItem<double>(distance);
                return readX;
            }
            else if (typeof(TIn) == typeof(SpatialDescendingMatchComparer))
            {
                if (comparer is not SpatialDescendingMatchComparer spatialDescendingMatchComparer)
                    goto Failed;
                
                var readX = reader.Read(fieldId, out (double lat, double lon) coordinates);
                var distance = SpatialUtils.GetGeoDistance(in coordinates, in spatialDescendingMatchComparer);

                storedValue = (TOut)(object)new NumericalItem<double>(distance);
                return readX; 
            }
            else if (typeof(TOut) == typeof(SequenceItem))
            {
                var readX = reader.Read(fieldId, out var sv);
                storedValue = (TOut)(object)new SequenceItem((byte*)Unsafe.AsPointer(ref sv[0]), sv.Length);
                return readX;
            }
            else if (typeof(TOut) == typeof(NumericalItem<long>))
            {
                var readX = reader.Read<long>(fieldId, out var value);
                storedValue = (TOut)(object)new NumericalItem<long>(value);
                return readX;
            }
            else if (typeof(TOut) == typeof(NumericalItem<double>))
            {
                var readX = reader.Read<double>(fieldId, out var value);
                storedValue = (TOut)(object)new NumericalItem<double>(value);
                return readX;
            }

            Failed:
            Unsafe.SkipInit(out storedValue);
            return false;
        }

        internal struct MultiMatchComparer<TComparer, TValue> : IComparer<MultiMatchComparer<TComparer, TValue>.Item>
            where TComparer : IMatchComparer
            where TValue : struct
        {
            private SortingMultiMatch<TInner, TComparer1, TComparer2, TComparer3, TComparer4, TComparer5, TComparer6, TComparer7, TComparer8, TComparer9> _multiMatch;

            public struct Item
            {
                public long Key;
                public TValue Value;
                public UnmanagedSpan Entry;
                public float Score;
            }

            public MultiMatchComparer(SortingMultiMatch<TInner, TComparer1, TComparer2, TComparer3, TComparer4, TComparer5, TComparer6, TComparer7, TComparer8, TComparer9> multiMatch)
            {
                _multiMatch = multiMatch;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public int Compare(Item ix, Item iy)
            {
                if (typeof(TComparer) == typeof(BoostingComparer))
                {
                    float score = iy.Score - ix.Score;
                    return Math.Abs(score) < Constants.Boosting.ScoreEpsilon ? 0 : Math.Sign(score);
                }
                else
                {
                    if (ix.Key > 0 && iy.Key > 0)
                    {
                        int result = Compare<TComparer1, TValue>(_multiMatch._comparer1, ix, iy);
                        if (result == 0)
                        {
                            // We will only call this when there is no other choice. 
                            result = _multiMatch._compareFuncs[0](ref _multiMatch, 0, ix.Entry, iy.Entry, ix.Score, iy.Score);
                        }

                        return result;
                    }
                    else if (ix.Key > 0)
                    {
                        return 1;
                    }

                    return -1;
                }
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public static int Compare<TInnerComparer, TIn> (TInnerComparer comparer, Item ix, Item iy)
                where TInnerComparer : IMatchComparer
                where TIn : struct
            {
                if (typeof(TInnerComparer) == typeof(BoostingComparer))
                {
                    float score = iy.Score - ix.Score;
                    return Math.Abs(score) < Constants.Boosting.ScoreEpsilon ? 0 : Math.Sign(score);
                }
                else if (typeof(TIn) == typeof(SequenceItem))
                {
                    return comparer.CompareSequence(
                        new ReadOnlySpan<byte>(((SequenceItem)(object)ix.Value).Ptr, ((SequenceItem)(object)ix.Value).Size),
                        new ReadOnlySpan<byte>(((SequenceItem)(object)iy.Value).Ptr, ((SequenceItem)(object)iy.Value).Size));
                }
                else if (typeof(TIn) == typeof(NumericalItem<long>))
                {
                    return comparer.CompareNumerical(((NumericalItem<long>)(object)ix.Value).Value, ((NumericalItem<long>)(object)iy.Value).Value);
                }
                else if (typeof(TIn) == typeof(NumericalItem<double>))
                {
                    return comparer.CompareNumerical(((NumericalItem<double>)(object)ix.Value).Value, ((NumericalItem<double>)(object)iy.Value).Value);
                }
                return -1;
            }
        }

        [SkipLocalsInit]
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private int Fill<TOut>(Span<long> matches) 
            where TOut : struct
        {
            // Important: If you are going to request a massive take like 20K you need to pass at least a 20K size buffer to work with.
            //            The rationale for such behavior is that sorting has to find among the candidates the order between elements,
            //            and it can't do so without checking every single element found. If you fail to do so, your results may not be
            //            correct. 
            Debug.Assert(_take <= matches.Length);

            int totalMatches = _inner.Fill(matches);
            if (totalMatches == 0)
                return 0;

            int take = _take <= 0 ? matches.Length : Math.Min(matches.Length, _take);
            TotalResults += totalMatches;

            int floatArraySize = 2 * sizeof(float) * matches.Length;
            int matchesArraySize = sizeof(long) * matches.Length;
            int itemArraySize = 2 * Unsafe.SizeOf<MultiMatchComparer<TComparer1, TOut>.Item>() * matches.Length;
            using var _ = _searcher.Allocator.Allocate(itemArraySize + matchesArraySize + floatArraySize, out var bufferHolder);

            var matchesKeysSpan = MemoryMarshal.Cast<byte, MultiMatchComparer<TComparer1, TOut>.Item>(bufferHolder.ToSpan().Slice(0, itemArraySize));
            Debug.Assert(matchesKeysSpan.Length == 2 * matches.Length);

            // PERF: We want to avoid to share cache lines, that's why the second array will move toward the end of the array. 
            var matchesKeys = matchesKeysSpan[0..matches.Length];
            var bKeys = matchesKeysSpan[^matches.Length..];

            Span<float> allScoresValues = MemoryMarshal.Cast<byte, float>(bufferHolder.ToSpan().Slice(itemArraySize, floatArraySize));
            var matchesScores = allScoresValues[..matches.Length];
            var bScores = allScoresValues[^matches.Length..];

            if (HasBoostingComparer)
            {
                // Initializing the scores and retrieve them.
                matchesScores.Fill(1);
                _inner.Score(matches[0..totalMatches], matchesScores[0..totalMatches]);
            }

            var searcher = _searcher;
            var fieldId = typeof(TComparer1) != typeof(BoostingComparer) ? _comparer1.FieldId : 0;
            var comparer = new MultiMatchComparer<TComparer1, TOut>(this);            
            for (int i = 0; i < totalMatches; i++)
            {
                UnmanagedSpan matchIndexEntry = searcher.GetIndexEntryPointer(matches[i]);
                var read = typeof(TComparer1) == typeof(BoostingComparer) || 
                           Get(new IndexEntryReader(matchIndexEntry), fieldId, matches[i], out matchesKeys[i].Value, in _comparer1);
                matchesKeys[i].Key = read ? matches[i] : -matches[i];
                matchesKeys[i].Entry = matchIndexEntry;

                if (HasBoostingComparer)
                    matchesKeys[i].Score = matchesScores[i];
            }
            
            // We sort the first batch. That will also mean that we will sort the indexes too. 
            var sorter = new Sorter<MultiMatchComparer<TComparer1, TOut>.Item, long, MultiMatchComparer<TComparer1, TOut>>(comparer);
            sorter.Sort(matchesKeys[0..totalMatches], matches);

            Span<long> bValues = MemoryMarshal.Cast<byte, long>(bufferHolder.ToSpan().Slice(floatArraySize + itemArraySize, matchesArraySize));
            Debug.Assert(bValues.Length == matches.Length);

            while (true)
            {
                // We get a new batch
                int bTotalMatches = _inner.Fill(bValues);
                TotalResults += bTotalMatches;

                // When we don't have any new batch, we are done.
                if (bTotalMatches == 0)
                {
                    return totalMatches;
                }

                if (HasBoostingComparer)
                {
                    // Initialize the scores and retrieve scores from the new batch. 
                    bScores.Fill(1);
                    _inner.Score(bValues[0..bTotalMatches], bScores[0..bTotalMatches]);
                }

                // We get the keys to sort.
                for (int i = 0; i < bTotalMatches; i++)
                {
                    UnmanagedSpan matchIndexEntry = searcher.GetIndexEntryPointer(matches[i]);
                    var read = Get(new IndexEntryReader(matchIndexEntry), fieldId, bValues[i], out bKeys[i].Value, in _comparer1);
                    bKeys[i].Key = read ? bValues[i] : -bValues[i];
                    bKeys[i].Entry = matchIndexEntry;
                    
                    if (HasBoostingComparer)
                        bKeys[i].Score = bScores[i];
                }

                int bIdx = 0;
                int kIdx = 0;

                // Get rid of all the elements that are bigger than the last one.
                ref var lastElement = ref matchesKeys[take - 1];
                for (; bIdx < bTotalMatches; bIdx++)
                {
                    if (comparer.Compare(lastElement, bKeys[bIdx]) >= 0)
                    {
                        bKeys[kIdx] = bKeys[bIdx];
                        
                        kIdx++;
                    }
                        
                }
                bTotalMatches = kIdx;

                // We sort the new batch
                sorter.Sort(bKeys[0..bTotalMatches], bValues);

                // We merge both batches. 
                int aTotalMatches = Math.Min(totalMatches, take);

                int aIdx = aTotalMatches;
                bIdx = 0;
                kIdx = 0;

                while (aIdx > 0 && aIdx >= aTotalMatches / 8)
                {
                    // If the 'bigger' of what we had is 'bigger than'
                    if (comparer.Compare(matchesKeys[aIdx - 1], bKeys[0]) <= 0)
                        break;

                    aIdx /= 2;
                }

                // This is the new start location on the matches. 
                kIdx = aIdx;

                // If we bailed on the first check, nothing to do here. 
                if (aIdx == aTotalMatches - 1 || kIdx >= take)
                    goto End;

                // PERF: This can be improved with TimSort like techniques (Galloping) but given the amount of registers and method calls
                //       involved requires careful timing to understand if we are able to gain vs a more compact code and predictable
                //       memory access patterns. 

                while (aIdx < aTotalMatches && bIdx < bTotalMatches && kIdx < take)
                {
                    var result = comparer.Compare(matchesKeys[aIdx], bKeys[bIdx]);                    
                    if (result == 0)
                    {
                        // We will only call this when there is no other choice. 
                        ref var aItem = ref matchesKeys[aIdx];
                        ref var bItem = ref matchesKeys[bIdx];
                        result = _compareFuncs[0](ref this, 0, aItem.Entry, bItem.Entry, aItem.Score, bItem.Score);
                    }

                    if (result < 0)
                    {
                        matches[kIdx] = matchesKeys[aIdx].Key;
                        aIdx++;
                    }
                    else
                    {
                        matches[kIdx] = bKeys[bIdx].Key;
                        matchesKeys[kIdx] = bKeys[bIdx];
                        bIdx++;
                    }
                    kIdx++;
                }

                // If there is no more space in the buffer, discard everything else.
                if (kIdx >= take)
                    goto End;

                // PERF: We could improve this with a CopyTo (won't do that for now). 

                // Copy the rest, given that we have failed on one of the other 2 only a single one will execute.
                while (aIdx < aTotalMatches && kIdx < take)
                {
                    matches[kIdx++] = matchesKeys[aIdx++].Key;
                }

                while (bIdx < bTotalMatches && kIdx < take)
                {
                    matches[kIdx] = bKeys[bIdx].Key;
                    matchesKeys[kIdx] = bKeys[bIdx]; // We are using a new key, therefore we have to update it. 
                    kIdx++;
                    bIdx++;
                }

            End:
                totalMatches = kIdx;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static IMatchComparer GetComparer(ref SortingMultiMatch<TInner, TComparer1, TComparer2, TComparer3, TComparer4, TComparer5, TComparer6, TComparer7, TComparer8, TComparer9> current, int comparerIdx)
        {
            switch(comparerIdx)
            {
                case 0: return current._comparer2;
                case 1: return current._comparer3;
                case 2: return current._comparer4;
                case 3: return current._comparer5;
                case 4: return current._comparer6;
                case 5: return current._comparer7;
                case 6: return current._comparer8;
                case 7: return current._comparer9;
            }

            throw new NotSupportedException("MultiMatchComparer only support up to 9 different comparisons.");
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
        private static int CompareSpatialRelaxation<TComparer>(
            ref SortingMultiMatch<TInner, TComparer1, TComparer2, TComparer3, TComparer4, TComparer5, TComparer6, TComparer7, TComparer8, TComparer9> current,
            int comparerIdx, UnmanagedSpan item1, UnmanagedSpan item2, float scoreItem1, float scoreItem2)
            where TComparer : IMatchComparer
        {
            if (typeof(TComparer) == typeof(SpatialAscendingMatchComparer))
                return CompareSpatial<SpatialAscendingMatchComparer>(ref current, comparerIdx, item1, item2, scoreItem1, scoreItem2);
            return CompareSpatial<SpatialDescendingMatchComparer>(ref current, comparerIdx, item1, item2, scoreItem1, scoreItem2);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
        private static int CompareSpatial<TComparer>(
            ref SortingMultiMatch<TInner, TComparer1, TComparer2, TComparer3, TComparer4, TComparer5, TComparer6, TComparer7, TComparer8, TComparer9> current,
            int comparerIdx, UnmanagedSpan item1, UnmanagedSpan item2, float scoreItem1, float scoreItem2)
        where TComparer : ISpatialComparer, IMatchComparer
        {
            var comparer = (TComparer)GetComparer(ref current, comparerIdx);
            var readerX = new IndexEntryReader(item1);
            var readX = readerX.Read(comparer.FieldId, out (double lat, double lon) resultX);

            var readerY = new IndexEntryReader(item2);
            var readY = readerY.Read(comparer.FieldId, out (double lat, double lon) resultY);

            if (readX && readY)
            {
                var readerXDistance = SpatialUtils.GetGeoDistance(in resultX, in comparer);
                var readerYDistance = SpatialUtils.GetGeoDistance(in resultY, in comparer);

                var result = comparer.CompareNumerical(readerXDistance, readerYDistance);
                int nextComparer = comparerIdx + 1;
                if (result == 0 && nextComparer < current._totalComparers)
                {
                    return current._compareFuncs[nextComparer](ref current, nextComparer, item1, item2, scoreItem1, scoreItem2);
                }
            }
            else if (readX)
                return -1;

            return 1;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
        private static int CompareSequence<TComparer>(ref SortingMultiMatch<TInner, TComparer1, TComparer2, TComparer3, TComparer4, TComparer5, TComparer6, TComparer7, TComparer8, TComparer9> current, int comparerIdx, UnmanagedSpan item1, UnmanagedSpan item2, float scoreItem1, float scoreItem2)
            where TComparer : IMatchComparer
        {
            var comparer = (TComparer)GetComparer(ref current, comparerIdx);
            var comp1Reader = new IndexEntryReader(item1);
            var comp2Reader = new IndexEntryReader(item2);

            bool read1 = comp1Reader.Read(comparer.FieldId, out var sv1);
            bool read2 = comp2Reader.Read(comparer.FieldId, out var sv2);
            if (read1 && read2)
            {
                var result = comparer.CompareSequence(sv1, sv2);
                int nextComparer = comparerIdx + 1;
                if (result == 0 && nextComparer < current._totalComparers)
                {
                    return current._compareFuncs[nextComparer](ref current, nextComparer, item1, item2, scoreItem1, scoreItem2);
                }
                return result;
            }

            if (read1)
                return -1;
            return 1;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
        private static int CompareNumerical<TComparer, T>(ref SortingMultiMatch<TInner, TComparer1, TComparer2, TComparer3, TComparer4, TComparer5, TComparer6, TComparer7, TComparer8, TComparer9> current, int comparerIdx, UnmanagedSpan item1, UnmanagedSpan item2, float scoreItem1, float scoreItem2)
            where TComparer : IMatchComparer
        {
            var comparer = (TComparer)GetComparer(ref current, comparerIdx);
            var comp1Reader = new IndexEntryReader(item1);
            var comp2Reader = new IndexEntryReader(item2);

            bool read1, read2;

            if (typeof(T) == typeof(long))
            {
                read1 = comp1Reader.Read<long>(comparer.FieldId, out var si1);
                read2 = comp2Reader.Read<long>(comparer.FieldId, out var si2);
                if (read1 && read2)
                {
                    var result = comparer.CompareNumerical(si1, si2);
                    int nextComparer = comparerIdx + 1;
                    if (result == 0 && nextComparer < current._totalComparers)
                    {
                        return current._compareFuncs[nextComparer](ref current, nextComparer, item1, item2, scoreItem1, scoreItem2);
                    }
                    return result;
                }
            }
            else
            {
                read1 = comp1Reader.Read<double>(comparer.FieldId, out var sd1);
                read2 = comp2Reader.Read<double>(comparer.FieldId, out var sd2);
                if (read1 && read2)
                {
                    var result = comparer.CompareNumerical<double>(sd1, sd2);
                    int nextComparer = comparerIdx + 1;
                    if (result == 0 && nextComparer < current._totalComparers)
                    {
                        return current._compareFuncs[nextComparer](ref current, nextComparer, item1, item2, scoreItem1, scoreItem2);
                    }
                    return result;
                }
            }

            if (read1)
                return -1;
            return 1;         
        }

        public void Score(Span<long> matches, Span<float> scores) 
        {
            throw new NotSupportedException($"Scoring is not supported by {nameof(SortingMultiMatch)}");
        }

        public QueryInspectionNode Inspect()
        {
            var comparers = typeof(TComparer1).Name;
            if (typeof(TComparer2) != typeof(SortingMultiMatch.NullComparer))
                comparers += $"|{typeof(TComparer2).Name}";
            if (typeof(TComparer3) != typeof(SortingMultiMatch.NullComparer))
                comparers += $"|{typeof(TComparer3).Name}";
            if (typeof(TComparer4) != typeof(SortingMultiMatch.NullComparer))
                comparers += $"|{typeof(TComparer4).Name}";
            if (typeof(TComparer5) != typeof(SortingMultiMatch.NullComparer))
                comparers += $"|{typeof(TComparer5).Name}";
            if (typeof(TComparer6) != typeof(SortingMultiMatch.NullComparer))
                comparers += $"|{typeof(TComparer6).Name}";
            if (typeof(TComparer7) != typeof(SortingMultiMatch.NullComparer))
                comparers += $"|{typeof(TComparer7).Name}";
            if (typeof(TComparer8) != typeof(SortingMultiMatch.NullComparer))
                comparers += $"|{typeof(TComparer8).Name}";
            if (typeof(TComparer9) != typeof(SortingMultiMatch.NullComparer))
                comparers += $"|{typeof(TComparer9).Name}";

            return new QueryInspectionNode($"{nameof(SortingMatch)} [{comparers}]",
                    children: new List<QueryInspectionNode> { _inner.Inspect() },
                    parameters: new Dictionary<string, string>()
                    {
                        { nameof(IsBoosting), IsBoosting.ToString() },
                    });
        }

        string DebugView => Inspect().ToString();
    }
}
