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
    public unsafe struct SortingMatch<TInner, TComparer> : IQueryMatch
        where TInner : IQueryMatch
        where TComparer : struct, IMatchComparer
    {
        private readonly IndexSearcher _searcher;
        private IQueryMatch _inner;        
        private readonly TComparer _comparer;
        private readonly int _take;
        private readonly bool _isScoreComparer;
        private readonly delegate*<ref SortingMatch<TInner, TComparer>, Span<long>, int> _fillFunc;

        private const int NotStarted = -1;
        private int _currentIdx;

        internal byte* _buffer;
        internal int _bufferSize;
        internal IDisposable _bufferHandler;

        public long TotalResults;

        public SortingMatch(IndexSearcher searcher, in TInner inner, in TComparer comparer, int take = -1)
        {
            _searcher = searcher;
            _inner = inner;
            _take = take;
            _comparer = comparer;
            _isScoreComparer = typeof(TComparer) == typeof(BoostingComparer);
            _currentIdx = NotStarted;

            TotalResults = 0;

            if (typeof(TComparer) == typeof(BoostingComparer))
            {
                _fillFunc = &Fill<BoostingSorting>;
            }
            else
            {
                _fillFunc = _comparer.FieldType switch
                {
                    MatchCompareFieldType.Sequence or MatchCompareFieldType.Alphanumeric => &FillAdv<Fetcher,SequenceItem>,
                    MatchCompareFieldType.Integer => &Fill<ItemSorting<NumericalItem<long>>>,
                    MatchCompareFieldType.Floating => &Fill<ItemSorting<NumericalItem<double>>>,
                    MatchCompareFieldType.Spatial => &Fill<ItemSorting<NumericalItem<double>>>,
                    MatchCompareFieldType.Score => &Fill<BoostingSorting>,
                    _ => throw new ArgumentOutOfRangeException(_comparer.FieldType.ToString())
                };
            }
        }
        
        private interface IFetcher
        {
            bool Get<TOut, TIn>(IndexSearcher searcher, FieldMetadata binding, long entryId, out TOut storedValue, in TIn comparer)
                where TIn : IMatchComparer
                where TOut : struct;
        }

        private struct Fetcher : IFetcher
        {
             [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public bool Get<TOut, TIn>(IndexSearcher searcher, FieldMetadata binding, long entryId, out TOut storedValue, in TIn comparer)
                where TIn : IMatchComparer
                where TOut : struct
            {
                var reader = searcher.GetEntryReaderFor(entryId);

                if (typeof(TIn) == typeof(SpatialAscendingMatchComparer))
                {
                    if (comparer is not SpatialAscendingMatchComparer spatialAscendingMatchComparer)
                        goto Failed;

                    var readX = reader.GetFieldReaderFor(binding).Read(out (double lat, double lon) coordinates);
                    if (readX == false)
                        goto Failed;
                        
                    var distance = SpatialUtils.GetGeoDistance(in coordinates, in spatialAscendingMatchComparer);

                    storedValue = (TOut)(object)new NumericalItem<double>(distance);
                    return readX;
                }
                else if (typeof(TIn) == typeof(SpatialDescendingMatchComparer))
                {
                    if (comparer is not SpatialDescendingMatchComparer spatialDescendingMatchComparer)
                        goto Failed;

                    var readX = reader.GetFieldReaderFor(binding).Read(out (double lat, double lon) coordinates);
                    if (readX == false)
                        goto Failed;
                    
                    var distance = SpatialUtils.GetGeoDistance(in coordinates, in spatialDescendingMatchComparer);

                    storedValue = (TOut)(object)new NumericalItem<double>(distance);
                    return readX;
                }
                else if (typeof(TOut) == typeof(SequenceItem))
                {
                    var readX = reader.GetFieldReaderFor(binding).Read(out var sv);
                    if (readX == false)
                        goto Failed;
                    
                    fixed (byte* svp = sv)
                    {
                        storedValue = (TOut)(object)new SequenceItem(svp, sv.Length);
                    }
                    return readX;
                }
                else if (typeof(TOut) == typeof(NumericalItem<long>))
                {
                    var readX = reader.GetFieldReaderFor(binding).Read<long>(out var value);
                    if (readX == false)
                        goto Failed;
                    
                    storedValue = (TOut)(object)new NumericalItem<long>(value);
                    return readX;
                }
                else if (typeof(TOut) == typeof(NumericalItem<double>))
                {
                    var readX = reader.GetFieldReaderFor(binding).Read<double>(out var value);
                    if (readX == false)
                        goto Failed;
                    
                    storedValue = (TOut)(object)new NumericalItem<double>(value);
                    return readX;
                }

                Failed:
                Unsafe.SkipInit(out storedValue);
                return false;
            }
        }

        private static int FillAdv<TFetcher, TOut>(ref SortingMatch<TInner, TComparer> match, Span<long> matches)
            where TFetcher : struct, IFetcher
            where TOut : struct
        {
            // We will try to use the matches buffer for as long as we can, if it is not enough we will switch to a more complex 
            // behavior. This method should also be re-entrant for the case where we have already pre-sorted everything and 
            // we will just need to acquire via pages the totality of the results. 
            int totalMatches;
            if (match._currentIdx != NotStarted)
            {
                totalMatches = (int)match.TotalResults;
                goto ReturnMatches;
            }
            
            Debug.Assert(matches.Length > 1);
            totalMatches = match._take == -1 ? matches.Length : match._take;

            // We will allocate the space for the heap data structure that we are gonna use to fill the results.
            int heapSize = Unsafe.SizeOf<MatchComparer<TComparer, TOut>.Item>() * totalMatches;
            if (match._bufferSize < heapSize)
                match.UnlikelyGrowBuffer(heapSize);

            var items = new Span<MatchComparer<TComparer, TOut>.Item>(match._buffer, totalMatches);
            match._currentIdx = 0;

            // Initialize the important infrastructure for the sorting.
            TFetcher fetcher = default;
            var comparer = new MatchComparer<TComparer, TOut>(match._comparer);
            var index = 0;
            while (true)
            {
                var read = match._inner.Fill(matches);
                match.TotalResults += read;
                if (read == 0)
                    break;

                for (int i = 0; i < read; i++)
                {
                    var cur = new MatchComparer<TComparer, TOut>.Item { Key = matches[i], };
                    if (fetcher.Get(match._searcher, match._comparer.Field, cur.Key, out cur.Value, match._comparer) == false)
                    {
                        //TODO: What happens when there aren't any values for the field? 
                        continue;
                    }

                    if (index < items.Length)
                    {
                        items[index++] = cur;
                    }
                    else if (comparer.Compare(items[^1], cur) > 0)
                    {
                        items[^1] = cur;
                    }
                    else
                    {
                        continue;
                    }

                    HeapUp(items);

                    [MethodImpl(MethodImplOptions.AggressiveInlining)]
                    void HeapUp(Span<MatchComparer<TComparer, TOut>.Item> items)
                    {
                        var current = index - 1;
                        while (current > 0)
                        {
                            var parent = (current - 1) / 2;
                            if (comparer.Compare(items[parent], items[current]) <= 0)
                                break;

                            (items[parent], items[current]) = (items[current], items[parent]);
                            current = parent;
                        }
                    }
                }

                match.TotalResults = index;
            }

            ReturnMatches:
            items = new Span<MatchComparer<TComparer, TOut>.Item>(match._buffer, (int)match.TotalResults);

            int matchesToReturn = 0;
            int leftOvers = Math.Min(match._currentIdx + matches.Length, (int)match.TotalResults);
            for (int i = match._currentIdx; i < leftOvers; i++)
            {
                matches[matchesToReturn] = items[i].Key;
                matchesToReturn++;
            }
            match._currentIdx += matchesToReturn;
            return matchesToReturn;
        }

        public long Count => _inner.Count;

        public QueryCountConfidence Confidence => throw new NotSupportedException();

        public bool IsBoosting => _inner.IsBoosting || _isScoreComparer;

        public int AndWith(Span<long> buffer, int matches)
        {
            throw new NotSupportedException($"{nameof(SortingMatch<TInner, TComparer>)} does not support the operation of {nameof(AndWith)}.");
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Fill(Span<long> matches)
        {
            return _fillFunc(ref this, matches);
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
            _buffer = buffer.Ptr;
            _bufferHandler = bufferHandler;
        }

        private interface IItemSorter
        {
            int Execute(ref SortingMatch<TInner, TComparer> match, Span<long> matches);
        }

        private struct ItemSorting<TOut> : IItemSorter
            where TOut : struct
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            private static bool Get<TIn>(IndexSearcher searcher, FieldMetadata binding, long entryId, out TOut storedValue, in TIn comparer)
                where TIn : IMatchComparer
            {
                var fieldReader = searcher
                    .GetEntryReaderFor(entryId)
                    .GetFieldReaderFor(binding);
                
                if (fieldReader.Type is IndexEntryFieldType.Null)
                    goto Failed;

                if (typeof(TIn) == typeof(SpatialAscendingMatchComparer))
                {
                    if (comparer is not SpatialAscendingMatchComparer spatialAscendingMatchComparer)
                        goto Failed;

                    var readX = fieldReader.Read(out (double lat, double lon) coordinates);
                    if (readX == false)
                        goto Failed;
                        
                    var distance = SpatialUtils.GetGeoDistance(in coordinates, in spatialAscendingMatchComparer);

                    storedValue = (TOut)(object)new NumericalItem<double>(distance);
                    return readX;
                }
                else if (typeof(TIn) == typeof(SpatialDescendingMatchComparer))
                {
                    if (comparer is not SpatialDescendingMatchComparer spatialDescendingMatchComparer)
                        goto Failed;

                    var readX = fieldReader.Read(out (double lat, double lon) coordinates);
                    if (readX == false)
                        goto Failed;
                    
                    var distance = SpatialUtils.GetGeoDistance(in coordinates, in spatialDescendingMatchComparer);

                    storedValue = (TOut)(object)new NumericalItem<double>(distance);
                    return readX;
                }
                else if (typeof(TOut) == typeof(SequenceItem))
                {
                    var readX = fieldReader.Read(out var sv);
                    if (readX == false)
                        goto Failed;
                    
                    fixed (byte* svp = sv)
                    {
                        storedValue = (TOut)(object)new SequenceItem(svp, sv.Length);
                    }
                    return readX;
                }
                else if (typeof(TOut) == typeof(NumericalItem<long>))
                {
                    var readX = fieldReader.Read<long>(out var value);
                    if (fieldReader.Type is not IndexEntryFieldType.Tuple || readX == false)
                        goto Failed;
                    
                    storedValue = (TOut)(object)new NumericalItem<long>(value);
                    return readX;
                }
                else if (typeof(TOut) == typeof(NumericalItem<double>))
                {
                    var readX = fieldReader.Read<double>(out var value);
                    if (fieldReader.Type is not IndexEntryFieldType.Tuple || readX == false)
                        goto Failed;
                    
                    storedValue = (TOut)(object)new NumericalItem<double>(value);
                    return readX;
                }

                Failed:
                Unsafe.SkipInit(out storedValue);
                return false;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public int Execute(ref SortingMatch<TInner, TComparer> match, Span<long> matches)
            {
                // PERF: This method assumes that we need to perform sorting of the whole set (even if we have a take statement).
                // However, there is space to improve this further doing iterative sorting with element discarding in cases where
                // take statements requires us to remove lots of elements. While we are not going to perform this optimization yet
                // sorting is a costly primitive that if we can avoid it, would provide us a great opportunity for improvement. 

                // It may happen that multiple calls to `.Fill()` would require us to remove duplicates. While sorting is not 
                // required at this stage, to remove duplicates we need to sort anyways as we want to avoid further work down the line.
                var totalMatches = Sorting.SortAndRemoveDuplicates(matches);

                int itemArraySize = Unsafe.SizeOf<MatchComparer<TComparer, TOut>.Item>() * totalMatches;
                using var _ = match._searcher.Allocator.Allocate(itemArraySize, out var bufferHolder);

                var itemKeys = MemoryMarshal.Cast<byte, MatchComparer<TComparer, TOut>.Item>(bufferHolder.ToSpan().Slice(0, itemArraySize));
                Debug.Assert(itemKeys.Length == totalMatches);

                var searcher = match._searcher;
                var field = match._comparer.Field;
                var comparer = new MatchComparer<TComparer, TOut>(match._comparer);
                var indexContainsField = false;
                for (int i = 0; i < totalMatches; i++)
                {
                    var read = Get(searcher, field, matches[i], out itemKeys[i].Value, match._comparer);
                    itemKeys[i].Key = read ? matches[i] : -matches[i];
                    indexContainsField |= read;
                }

                // We sort the the set
                if (indexContainsField)
                {
                    var sorter = new Sorter<MatchComparer<TComparer, TOut>.Item, long, MatchComparer<TComparer, TOut>>(comparer);
                    sorter.Sort(itemKeys, matches[0..totalMatches]);
                }

                // We have a take statement so we are only going to care about the highest priority elements. 
                if (match._take > 0)
                    totalMatches = Math.Min(totalMatches, match._take);

                return totalMatches;
            }
        }

        private struct BoostingSorting : IItemSorter
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public int Execute(ref SortingMatch<TInner, TComparer> match, Span<long> matches)
            {
                // PERF: This method assumes that we need to perform sorting of the whole set (even if we have a take statement).
                // However, there is space to improve this further doing iterative sorting with element discarding in cases where
                // take statements requires us to remove lots of elements. While we are not going to perform this optimization yet
                // sorting is a costly primitive that if we can avoid it, would provide us a great opportunity for improvement. 

                // It may happen that multiple calls to `.Fill()` would require us to remove duplicates. While sorting is not 
                // required at this stage, to remove duplicates we need to sort anyways as we want to avoid further work down the line. 
                var totalMatches = Sorting.SortAndRemoveDuplicates(matches);

                int scoresArraySize = sizeof(float) * totalMatches;
                using var _ = match._searcher.Allocator.Allocate( scoresArraySize, out var bufferHolder);

                var scores = MemoryMarshal.Cast<byte, float>(bufferHolder.ToSpan());
                Debug.Assert(scores.Length == totalMatches);
                scores.Fill(Bm25Relevance.InitialScoreValue);
                
                // We perform the scoring process. 
                match._inner.Score(matches[0..totalMatches], scores, 1f);

                // If we need to do documents boosting then we need to modify the based on documents stored score. 
                if (match._searcher.DocumentsAreBoosted)
                {
                    // We get the boosting tree and go to check every document. 
                    var tree = match._searcher.GetDocumentBoostTree();
                    if (tree is { NumberOfEntries: > 0 })
                    {
                        // We are going to read from the boosting tree all the boosting values and apply that to the scores array.
                        for (int idx = 0; idx < totalMatches; idx++)
                        {
                            var ptr = (float*)tree.ReadPtr(matches[idx], out var _);
                            if (ptr == null)
                                continue;

                            scores[idx] *= *ptr;
                        }
                    }
                }

                // Before returning we want the return the matches in score order. 
                var sorter = new Sorter<float, long, NumericDescendingComparer>();
                sorter.Sort(scores, matches[0..totalMatches]);

                // We have a take statement so we are only going to care about the highest priority elements. 
                if (match._take > 0)
                    totalMatches = Math.Min(totalMatches, match._take);

                return totalMatches;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static int Fill<TSorter>(ref SortingMatch<TInner, TComparer> match, Span<long> matches)
            where TSorter : struct, IItemSorter
        {
            // We will try to use the matches buffer for as long as we can, if it is not enough we will switch to a more complex 
            // behavior. This method should also be re-entrant for the case where we have already pre-sorted everything and 
            // we will just need to acquire via pages the totality of the results. 

            if (match._currentIdx != NotStarted)
                goto ReturnMatches;

            Debug.Assert(matches.Length > 1);
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

            long* buffer = (long*)match._buffer;
            while (true)
            {
                // We will ensure we have enough space to fill matches.
                int excessSpace = match._bufferSize - totalMatches;
                if (excessSpace < 128)
                {
                    match.UnlikelyGrowBuffer(match._bufferSize);
                    excessSpace = match._bufferSize - totalMatches;
                    
                    // Since the buffer is gonna grow, we need to update all local variables.
                    buffer = (long*)match._buffer;
                }

                // We will get more batches until we are done getting matches.
                int bTotalMatches = match._inner.Fill(new Span<long>(buffer + totalMatches, excessSpace));
                if (bTotalMatches == 0)
                {
                    // If we are done, we know that we can sort, remove duplicates and prepare the output buffer.
                    totalMatches = ((TSorter)default).Execute(ref match, new Span<long>(buffer, totalMatches));

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
        public void Score(Span<long> matches, Span<float> scores, float boostFactor) 
        {
        }

        public QueryInspectionNode Inspect()
        {
            return new QueryInspectionNode($"{nameof(SortingMatch)} [{typeof(TComparer).Name}]",
                children: new List<QueryInspectionNode> { _inner.Inspect()},
                parameters: new Dictionary<string, string>()
                {
                        { nameof(IsBoosting), IsBoosting.ToString() },
                });
        }

        string DebugView => Inspect().ToString();
    }
}
