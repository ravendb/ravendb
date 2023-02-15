using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Corax.Mappings;
using Corax.Utils;
using Corax.Utils.Spatial;
using Sparrow;
using Sparrow.Server.Utils;

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


        private const int NotStarted = -1;
        private int _currentIdx;

        internal long* _buffer;
        internal int _bufferSize;
        internal IDisposable _bufferHandler;

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
            _currentIdx = NotStarted;

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

            if (HasBoostingComparer)
            {
                //In Corax's implementation of ranking, we assume that the IDs in the `Score()` parameter are sorted.
                //This way, we can perform a BinarySearch to find the pair <Entry, Score> and append the score to it.
                //In the case of compound sorting, when boosting is not the main comparator, matches are not sorted because the previous comparator may have changed the order.
                //This would require a linear search, which can be extremely costly. Additionally, this would require changing the API of Scoring to indicate whether it's ordered or not.
                var boostingIsNotMainComparer = typeof(TComparer2) == typeof(BoostingComparer) || typeof(TComparer3) == typeof(BoostingComparer) ||
                                                typeof(TComparer4) == typeof(BoostingComparer) || typeof(TComparer5) == typeof(BoostingComparer) || typeof(TComparer6) == typeof(BoostingComparer) ||
                                                typeof(TComparer7) == typeof(BoostingComparer) || typeof(TComparer8) == typeof(BoostingComparer) || typeof(TComparer9) == typeof(BoostingComparer);
                if (boostingIsNotMainComparer)
                    throw new NotSupportedException(
                        $"{nameof(SortingMultiMatch)} can compare score only as main property. Queries like 'order by Field, [..], score(), [..] ' etc are not supported.");

            }
            
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
                MatchCompareFieldType.Sequence or MatchCompareFieldType.Alphanumeric => Fill<ItemSorting<SequenceItem>>(ref this, matches),
                MatchCompareFieldType.Integer => Fill<ItemSorting<NumericalItem<long>>>(ref this, matches),
                MatchCompareFieldType.Floating => Fill<ItemSorting<NumericalItem<double>>>(ref this, matches),
                MatchCompareFieldType.Spatial => Fill<ItemSorting<NumericalItem<double>>>(ref this, matches),
                MatchCompareFieldType.Score => Fill<ItemSorting<NumericalItem<float>>>(ref this, matches),
                _ => throw new ArgumentOutOfRangeException(_comparer1.FieldType.ToString())
            };
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

        [MethodImpl(MethodImplOptions.NoInlining)]
        private void UnlikelyGrowBuffer(int currentlyUsed)
        {
            // Calculate the new size. 
            int size = (int)(currentlyUsed * (currentlyUsed > 16 * Voron.Global.Constants.Size.Megabyte ? 1.5 : 2));

            // Allocate the new buffer
            var bufferHandler = _searcher.Allocator.Allocate(size * sizeof(long), out var buffer);

            // In case there exist already buffers in place, we copy the content.
            if (_buffer != null)
            {
                // Ensure we copy the content and then switch the buffers. 
                new Span<long>(_buffer, currentlyUsed).CopyTo(new Span<long>(buffer.Ptr, size));
                _bufferHandler.Dispose();
            }

            _bufferSize = size;
            _buffer = (long*)buffer.Ptr;
            _bufferHandler = bufferHandler;
        }

        private interface IItemSorter
        {
            int Execute(ref SortingMultiMatch<TInner, TComparer1, TComparer2, TComparer3, TComparer4, TComparer5, TComparer6, TComparer7, TComparer8, TComparer9> match, Span<long> matches);
        }

        private struct ItemSorting<TOut> : IItemSorter
            where TOut : struct
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            private static bool Get<TIn>(IndexEntryReader reader, FieldMetadata binding, out TOut storedValue, in TIn comparer)
                where TIn : IMatchComparer
            {
                if (typeof(TIn) == typeof(SpatialAscendingMatchComparer))
                {
                    if (comparer is not SpatialAscendingMatchComparer spatialAscendingMatchComparer)
                        goto Failed;

                    var readX = reader.GetFieldReaderFor(binding).Read(out (double lat, double lon) coordinates);
                    var distance = SpatialUtils.GetGeoDistance(in coordinates, in spatialAscendingMatchComparer);

                    storedValue = (TOut)(object)new NumericalItem<double>(distance);
                    return readX;
                }
                else if (typeof(TIn) == typeof(SpatialDescendingMatchComparer))
                {
                    if (comparer is not SpatialDescendingMatchComparer spatialDescendingMatchComparer)
                        goto Failed;

                    var readX = reader.GetFieldReaderFor(binding).Read(out (double lat, double lon) coordinates);
                    var distance = SpatialUtils.GetGeoDistance(in coordinates, in spatialDescendingMatchComparer);

                    storedValue = (TOut)(object)new NumericalItem<double>(distance);
                    return readX;
                }
                else if (typeof(TOut) == typeof(SequenceItem))
                {
                    var readX = reader.GetFieldReaderFor(binding).Read(out var sv);
                    fixed (byte* svp = sv)
                    {
                        storedValue = (TOut)(object)new SequenceItem(svp, sv.Length);
                    }

                    return readX;
                }
                else if (typeof(TOut) == typeof(NumericalItem<long>))
                {
                    var readX = reader.GetFieldReaderFor(binding).Read<long>(out var value);
                    storedValue = (TOut)(object)new NumericalItem<long>(value);
                    return readX;
                }
                else if (typeof(TOut) == typeof(NumericalItem<double>))
                {

                    var readX = reader.GetFieldReaderFor(binding).Read<double>(out var value);
                    storedValue = (TOut)(object)new NumericalItem<double>(value);
                    return readX;
                }

                Failed:
                Unsafe.SkipInit(out storedValue);
                return false;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public int Execute(ref SortingMultiMatch<TInner, TComparer1, TComparer2, TComparer3, TComparer4, TComparer5, TComparer6, TComparer7, TComparer8, TComparer9> match, Span<long> matches)
            {
                // PERF: This method assumes that we need to perform sorting of the whole set (even if we have a take statement).
                // However, there is space to improve this further doing iterative sorting with element discarding in cases where
                // take statements requires us to remove lots of elements. While we are not going to perform this optimization yet
                // sorting is a costly primitive that if we can avoid it, would provide us a great opportunity for improvement. 

                // It may happen that multiple calls to `.Fill()` would require us to remove duplicates. While sorting is not 
                // required at this stage, to remove duplicates we need to sort anyways as we want to avoid further work down the line.
                var totalMatches = Sorting.SortAndRemoveDuplicates(matches);

                int scoresArraySize = sizeof(float) * totalMatches;
                int itemArraySize = Unsafe.SizeOf<MultiMatchComparer<TComparer1, TOut>.Item>() * totalMatches;
                using var _ = match._searcher.Allocator.Allocate(itemArraySize + scoresArraySize, out var bufferHolder);

                var itemKeys = MemoryMarshal.Cast<byte, MultiMatchComparer<TComparer1, TOut>.Item>(bufferHolder.ToSpan().Slice(0, itemArraySize));
                Debug.Assert(itemKeys.Length == totalMatches);

                var scores = MemoryMarshal.Cast<byte, float>(bufferHolder.ToSpan().Slice(itemArraySize, scoresArraySize));
                Debug.Assert(scores.Length == totalMatches);

                if (match.HasBoostingComparer)
                {
                    // Initializing the scores and retrieve them.
                    scores.Fill(1);
                    match._inner.Score(matches, scores, 1f);
                }

                var searcher = match._searcher;
                var binding = typeof(TComparer1) != typeof(BoostingComparer) ? match._comparer1.Field : default;
                var comparer = new MultiMatchComparer<TComparer1, TOut>(match);
                for (int i = 0; i < totalMatches; i++)
                {
                    UnmanagedSpan matchIndexEntry = searcher.GetIndexEntryPointer(matches[i]);
                    var read = typeof(TComparer1) == typeof(BoostingComparer) ||
                               Get(new IndexEntryReader(matchIndexEntry.Address, matchIndexEntry.Length), binding, out itemKeys[i].Value, in match._comparer1);

                    itemKeys[i].Key = read ? matches[i] : -matches[i];
                    itemKeys[i].Entry = matchIndexEntry;
                    if (match.HasBoostingComparer)
                        itemKeys[i].Score = scores[i];
                }

                // We sort the first batch. That will also mean that we will sort the indexes too. 
                var sorter = new Sorter<MultiMatchComparer<TComparer1, TOut>.Item, long, MultiMatchComparer<TComparer1, TOut>>(comparer);
                sorter.Sort(itemKeys[0..totalMatches], matches);


                // We have a take statement so we are only going to care about the highest priority elements. 
                if (match._take > 0)
                    totalMatches = Math.Min(totalMatches, match._take);

                return totalMatches;
            }
        }


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static int Fill<TSorter>(ref SortingMultiMatch<TInner, TComparer1, TComparer2, TComparer3, TComparer4, TComparer5, TComparer6, TComparer7, TComparer8, TComparer9> match, Span<long> matches)
                where TSorter : struct, IItemSorter
        {
            // We will try to use the matches buffer for as long as we can, if it is not enough we will switch to a more complex 
            // behavior. This method should also be re-entrant for the case where we have already pre-sorted everything and 
            // we will just need to acquire via pages the totality of the results. 

            if (match._currentIdx != NotStarted)
                goto ReturnMatches;

            // If there is nothing or just 1 element to return, we are done as there is no need to sort anything.
            int totalMatches = match._inner.Fill(matches);
            if (totalMatches is 0 or 1)
            {
                match.TotalResults = totalMatches;
                match._currentIdx = totalMatches;
                return totalMatches;
            }

            // Here comes the most important part on why https://issues.hibernatingrhinos.com/issue/RavenDB-19718 
            // requires special handling of the situation of returning multiple pages. Sorting requires that we
            // guarantee a global ordering, which cannot be done unless we have the whole set available already.
            // Therefore, if the results are smaller than the buffer we were given to work with, everything
            // would be fine. However, there might exist the case where the total amount of hits is bigger than
            // the buffer we were given to work with. In those cases we have to recruit external memory to be able
            // to execute multiple `.Fill()` calls. 

            // To start with, we will try to get all the hits to sort and assume that the whole result-set will be
            // able to be stored in the matches buffer until we figure out that is not the case.

            bool isNotDone = true;
            if (totalMatches < matches.Length)
            {
                while (isNotDone)
                {
                    // We get a new batch
                    int bTotalMatches = match._inner.Fill(matches.Slice(totalMatches));
                    totalMatches += bTotalMatches;

                    // When we don't have any new batch, we are done.
                    if (bTotalMatches == 0)
                        isNotDone = false;

                    if (totalMatches > (matches.Length - matches.Length / 8))
                        break; // We are not done, therefore we will go to get extra temporary buffers.
                }
            }

            if (isNotDone == false)
            {
                // If we are done (the expected outcome), we know that we can do this on a single call. Therefore,
                // we will sort, remove duplicates and return the whole buffer.
                totalMatches = ((TSorter)default).Execute(ref match, matches[..totalMatches]);
                match.TotalResults = totalMatches;
                match._currentIdx = totalMatches;

                return totalMatches;
            }

            // However, it might happen that we are not actually done which means that we will need to recruit
            // an external buffer memory to temporarily store the sorted data and for that we need to estimate 
            // how much memory to recruit for doing so. 

            if (match._inner.Confidence >= QueryCountConfidence.Normal && match._inner.Count < Constants.Primitives.DefaultBufferSize)
            {
                // Since we expect to find less than the default buffer size, we just ask for that much. We divide by 2 because
                // the grow buffer function will adjust the used size to double the size. 
                match.UnlikelyGrowBuffer(Constants.Primitives.DefaultBufferSize / 2);
            }
            else
            {
                // Since confidence is not good recruiting four times the size of the current matches in external memory is a sensible
                // tradeoff, and we will just hit other grow sequences over time if needed. 
                match.UnlikelyGrowBuffer(Math.Max(128, 4 * matches.Length));
            }

            // Copy to the buffer the matches that we already have.
            matches[..totalMatches].CopyTo(new Span<long>(match._buffer, totalMatches));

            long* buffer = match._buffer;
            while (true)
            {
                // We will ensure we have enough space to fill matches.
                int excessSpace = match._bufferSize - totalMatches;
                if (excessSpace < 128)
                {
                    match.UnlikelyGrowBuffer(match._bufferSize);
                    excessSpace = match._bufferSize - totalMatches;
                }

                // We will get more batches until we are done getting matches.
                int bTotalMatches = match._inner.Fill(new Span<long>(buffer + totalMatches, excessSpace));
                if (bTotalMatches == 0)
                {
                    // If we are done, we know that we can sort, remove duplicates and prepare the output buffer.
                    totalMatches = ((TSorter)default).Execute(ref match, new Span<long>(match._buffer, totalMatches));

                    match.TotalResults = totalMatches;
                    match._currentIdx = 0;
                    goto ReturnMatches;
                }

                totalMatches += bTotalMatches;
            }

            ReturnMatches:

            if (match._currentIdx < match.TotalResults)
            {
                Debug.Assert(match._currentIdx != NotStarted);

                // We will just copy as many already sorted elements into the output buffer.
                int leftovers = Math.Min((int)(match.TotalResults - match._currentIdx), matches.Length);
                new Span<long>(match._buffer + match._currentIdx, leftovers).CopyTo(matches);
                match._currentIdx += leftovers;
                return leftovers;
            }

            // There are no more matches to return.
            return 0;
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
            var readerX = new IndexEntryReader(item1.Address, item1.Length);
            var readX = readerX.GetFieldReaderFor(comparer.Field).Read(out (double lat, double lon) resultX);

            var readerY = new IndexEntryReader(item2.Address, item2.Length);
            var readY = readerY.GetFieldReaderFor(comparer.Field).Read(out (double lat, double lon) resultY);

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
            var comp1Reader = new IndexEntryReader(item1.Address, item1.Length);
            var comp2Reader = new IndexEntryReader(item2.Address, item2.Length);

            bool read1 = comp1Reader.GetFieldReaderFor(comparer.Field).Read(out var sv1);
            bool read2 = comp2Reader.GetFieldReaderFor(comparer.Field).Read(out var sv2);
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
            var comp1Reader = new IndexEntryReader(item1.Address, item1.Length);
            var comp2Reader = new IndexEntryReader(item2.Address, item2.Length);

            bool read1, read2;

            if (typeof(T) == typeof(long))
            {
                read1 = comp1Reader.GetFieldReaderFor(comparer.Field).Read<long>(out var si1);
                read2 = comp2Reader.GetFieldReaderFor(comparer.Field).Read<long>(out var si2);
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
                read1 = comp1Reader.GetFieldReaderFor(comparer.Field).Read<double>(out var sd1);
                read2 = comp2Reader.GetFieldReaderFor(comparer.Field).Read<double>(out var sd2);
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

        public void Score(Span<long> matches, Span<float> scores, float boostFactor) 
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
