using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Corax.Mappings;
using Corax.Utils;
using Corax.Utils.Spatial;
using Sparrow;
using Sparrow.Server;
using Sparrow.Server.Utils;
using Voron.Data.Fixed;
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
        private readonly delegate*<ref SortingMatch<TInner, TComparer>, Span<long>, int> _fillFunc;

        private const int NotStarted = -1;
        private int _currentIdx;

        internal byte* _buffer;
        internal int _bufferSize;
        internal IDisposable _bufferHandler;

        internal int _bufferUsedCount;
        public long TotalResults;

        public SortingMatch(IndexSearcher searcher, in TInner inner, in TComparer comparer, int take = -1)
        {
            _searcher = searcher;
            _inner = inner;
            _take = take;
            _comparer = comparer;
            _bufferUsedCount = NotStarted;
            _currentIdx = NotStarted;

            TotalResults = 0;

            if (typeof(TComparer) == typeof(BoostingComparer))
            {
                _fillFunc = &Fill<Fetcher, NumericalItem<float>>;
            }
            else
            {
                _fillFunc = _comparer.FieldType switch
                {
                    MatchCompareFieldType.Sequence or MatchCompareFieldType.Alphanumeric => &Fill<Fetcher,SequenceItem>,
                    MatchCompareFieldType.Integer => &Fill<Fetcher, NumericalItem<long>>,
                    MatchCompareFieldType.Floating => &Fill<Fetcher, NumericalItem<double>>,
                    MatchCompareFieldType.Spatial => &Fill<Fetcher, NumericalItem<double>>,
                    MatchCompareFieldType.Score => &Fill<Fetcher, NumericalItem<double>>,
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
                if (typeof(TIn) == typeof(BoostingComparer))
                    throw new NotSupportedException($"Boosting fetching is done by the main {nameof(Fill)} method.");

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
                    return true;
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
                    return true;
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
                    return true;
                }
                else if (typeof(TOut) == typeof(NumericalItem<long>))
                {
                    var readX = reader.GetFieldReaderFor(binding).Read<long>(out var value);
                    if (readX == false)
                        goto Failed;
                    
                    storedValue = (TOut)(object)new NumericalItem<long>(value);
                    return true;
                }
                else if (typeof(TOut) == typeof(NumericalItem<double>))
                {
                    var readX = reader.GetFieldReaderFor(binding).Read<double>(out var value);
                    if (readX == false)
                        goto Failed;
                    
                    storedValue = (TOut)(object)new NumericalItem<double>(value);
                    return true;
                }
                else if (typeof(TOut) == typeof(NumericalItem<double>))
                {
                    var readX = reader.GetFieldReaderFor(binding).Read<double>(out var value);
                    if (readX == false)
                        goto Failed;
                    
                    storedValue = (TOut)(object)new NumericalItem<double>(value);
                    return true;
                }

                Failed:
                Unsafe.SkipInit(out storedValue);
                return false;
            }
        }

        private static int Fill<TFetcher, TOut>(ref SortingMatch<TInner, TComparer> match, Span<long> matches)
            where TFetcher : struct, IFetcher
            where TOut : struct
        {
            var comparer = new MatchComparer<TComparer, TOut>(match._comparer);

            ByteStringContext<ByteStringMemoryCache>.InternalScope bufferHandler = default;

            ref var matchesRef = ref MemoryMarshal.GetReference(matches);

            // This method should also be re-entrant for the case where we have already pre-sorted everything and 
            // we will just need to acquire via pages the totality of the results. 
            if (match._bufferUsedCount != NotStarted)
            {
                goto ReturnMatches;
            }
            
            Debug.Assert(matches.Length > 1);
            var totalMatches = match._take == -1 ? 
                Math.Max(128, matches.Length) : // no limit specified, we'll guess on the size and rely on growing as needed 
                match._take;

            // We will allocate the space for the heap data structure that we are gonna use to fill the results.
            int heapSize = Unsafe.SizeOf<MatchComparer<TComparer, TOut>.Item>() * totalMatches;
            if (match._bufferSize < heapSize)
                match.UnlikelyGrowBuffer(heapSize);

            var items = new Span<MatchComparer<TComparer, TOut>.Item>(match._buffer, totalMatches);

            Span<float> scores;
            if (typeof(TComparer) == typeof(BoostingComparer))
            {
                // Allocate the new buffer
                bufferHandler = match._searcher.Allocator.Allocate(matches.Length * sizeof(float), out var buffer);
                scores = new Span<float>(buffer.Ptr, matches.Length);
            }
            else
            {
                scores = Span<float>.Empty;
            }

            // Initialize the important infrastructure for the sorting.
            TFetcher fetcher = default;
            var index = 0;
            while (true)
            {
                var read = match._inner.Fill(matches);
                match.TotalResults += read;
                if (read == 0)
                    break;

                // Since we are doing boosting, we need to score the matches so we can use them later. 
                if (typeof(TComparer) == typeof(BoostingComparer))
                {
                    
                    Debug.Assert(scores.Length == matches.Length);

                    var readScores = scores[..read];
                    
                    // We have to initialize the score buffer with a positive number to ensure that multiplication (document-boosting) is taken into account when BM25 relevance returns 0 (for example, with AllEntriesMatch).
                    readScores.Fill(Bm25Relevance.InitialScoreValue);

                    // We perform the scoring process. 
                    match._inner.Score(matches[..read], readScores, 1f);

                    // If we need to do documents boosting then we need to modify the based on documents stored score. 
                    if (match._searcher.DocumentsAreBoosted)
                    {
                        // We get the boosting tree and go to check every document. 
                        var tree = match._searcher.GetDocumentBoostTree();
                        if (tree is { NumberOfEntries: > 0 })
                        {
                            // We are going to read from the boosting tree all the boosting values and apply that to the scores array.
                            ref var scoresRef = ref MemoryMarshal.GetReference(scores);
                            for (int idx = 0; idx < totalMatches; idx++)
                            {
                                var ptr = (float*)tree.ReadPtr(Unsafe.Add(ref matchesRef, idx), out var _);
                                if (ptr == null)
                                    continue;

                                ref var scoresIdx = ref Unsafe.Add(ref scoresRef, idx);
                                scoresIdx *= *ptr;                            }
                        }
                    }
                }

                // PERF: We perform this at the end to avoid this check if we are not really adding elements. 
                if (match._take == -1 && index + read + 16 > items.Length)
                {
                    // we don't have a limit to the number of results returned
                    // so we have to ensure that we keep *all* the results in memory, as such,
                    // we cannot limit the size of the sorting heap and need to grow it
                    match.UnlikelyGrowBuffer(match._bufferSize);
                    items = MemoryMarshal.Cast<byte, MatchComparer<TComparer, TOut>.Item>(new Span<byte>(match._buffer, match._bufferSize));
                }

                ref var itemsRef = ref MemoryMarshal.GetReference(items);
                ref var itemsLast = ref items[^1];

                MatchComparer<TComparer, TOut>.Item cur = default;
                for (int i = 0; i < read; i++)
                {
                    cur.Key = matches[i];
                    if (typeof(TComparer) == typeof(BoostingComparer))
                    {
                        // Since we are boosting, we can get the value straight from the scores array.
                        cur.Value = (TOut)(object)new NumericalItem<float>(scores[i]);
                    }
                    else
                    {
                        if (fetcher.Get(match._searcher, match._comparer.Field, cur.Key, out cur.Value, match._comparer) == false)
                            cur.Key = -cur.Key;
                    }

                    if (index < items.Length)
                    {
                        ref var itemPtr = ref Unsafe.Add(ref itemsRef, index);
                        itemPtr = cur;
                        index++;
                    }
                    else if (comparer.Compare(items[^1], cur) > 0)
                    {
                        itemsLast = cur;
                    }
                    else
                    {
                        continue;
                    }

                    HeapUp(ref itemsRef, index - 1);
                }
            }
            match._bufferUsedCount = index;

            ReturnMatches:
            items = new Span<MatchComparer<TComparer, TOut>.Item>(match._buffer, match._bufferUsedCount);

            int matchesToReturn = Math.Min(match._bufferUsedCount, matches.Length);
            ref var itemsStart = ref MemoryMarshal.GetReference(items);
            for (int i = 0; i < matchesToReturn; i++)
            {
                // We are getting the top element because this is a heap and the top element is the one that is
                // going to be removed. Then the rest of the element will be pushed up to find the new top.
                ref var matchIdxPtr = ref Unsafe.Add(ref matchesRef, i);
                matchIdxPtr = itemsStart.Key;
                HeapDown(ref itemsStart, match._bufferUsedCount - i - 1);
            }
            match._bufferUsedCount -= matchesToReturn;

            bufferHandler.Dispose();

            return matchesToReturn;
            
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            void HeapUp(ref MatchComparer<TComparer, TOut>.Item itemsStart, int current)
            {
                while (current > 0)
                {
                    var parent = (current - 1) / 2;
                    ref var parentItem = ref Unsafe.Add(ref itemsStart, parent);
                    ref var currentItem = ref Unsafe.Add(ref itemsStart, current);
                    if (comparer.Compare(ref parentItem, ref currentItem) <= 0)
                        break;
                        
                    (parentItem, currentItem) = (currentItem, parentItem);
                    current = parent;
                }
            }
            
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            void HeapDown(ref MatchComparer<TComparer, TOut>.Item itemsPtr, int heapMaxIndex)
            {
                itemsPtr = Unsafe.Add(ref itemsPtr, heapMaxIndex);
                var current = 0;
                int childIdx;
                
                while ((childIdx = 2 * current + 1) < heapMaxIndex)
                {
                    if (childIdx + 1 < heapMaxIndex)
                    {
                        if (comparer.Compare(ref Unsafe.Add(ref itemsPtr, childIdx), ref Unsafe.Add(ref itemsPtr, childIdx+1)) > 0)
                        {
                            childIdx++;
                        }
                    }
                    
                    ref var currentPtr = ref Unsafe.Add(ref itemsPtr, current);
                    ref var childPtr = ref Unsafe.Add(ref itemsPtr, childIdx);
                    if (comparer.Compare(ref currentPtr, ref childPtr) <= 0)
                        break;
                
                    (currentPtr, childPtr) = (childPtr, currentPtr);
                    current = childIdx;
                }
            }
        }

        public long Count => _inner.Count;

        public QueryCountConfidence Confidence => throw new NotSupportedException();

        public bool IsBoosting => _inner.IsBoosting || typeof(TComparer) == typeof(BoostingComparer);

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
