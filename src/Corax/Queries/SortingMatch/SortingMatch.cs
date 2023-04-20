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
        internal readonly TComparer _comparer;
        private readonly int _take;
        private readonly delegate*<ref SortingMatch<TInner, TComparer>, Span<long>, int> _fillFunc;

        private const int NotStarted = -1;

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
                    return true;
                }
                else if (typeof(TIn) == typeof(SpatialDescendingMatchComparer))
                {
                    if (comparer is not SpatialDescendingMatchComparer spatialDescendingMatchComparer)
                        goto Failed;

                    if (fieldReader.Type != IndexEntryFieldType.SpatialPoint)
                        goto Failed;
                    
                    var readX = fieldReader.Read(out (double lat, double lon) coordinates);
                    
                    if (readX == false)
                        goto Failed;
                    
                    var distance = SpatialUtils.GetGeoDistance(in coordinates, in spatialDescendingMatchComparer);

                    storedValue = (TOut)(object)new NumericalItem<double>(distance);
                    return true;
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
                    return true;
                }
                else if (typeof(TOut) == typeof(NumericalItem<long>))
                {
                    var readX = fieldReader.Read<long>(out var value);
                    if (fieldReader.Type is not IndexEntryFieldType.Tuple || readX == false)
                        goto Failed;
                    
                    storedValue = (TOut)(object)new NumericalItem<long>(value);
                    return true;
                }
                else if (typeof(TOut) == typeof(NumericalItem<double>))
                {
                    var readX = fieldReader.Read<double>(out var value);
                    if (fieldReader.Type is not IndexEntryFieldType.Tuple || readX == false)
                        goto Failed;
                    
                    storedValue = (TOut)(object)new NumericalItem<double>(value);
                    return true;
                }
                else if (typeof(TOut) == typeof(NumericalItem<double>))
                {
                    var readX = fieldReader.Read<double>(out var value);
                    if (fieldReader.Type is not IndexEntryFieldType.Tuple || readX == false)
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

            // This method should also be re-entrant for the case where we have already pre-sorted everything and 
            // we will just need to acquire via pages the totality of the results. 
            if (match._bufferUsedCount != NotStarted)
            {
                goto ReturnMatches;
            }
            
            ByteStringContext<ByteStringMemoryCache>.InternalScope bufferHandler = default;
            Debug.Assert(matches.Length > 1);
            var totalMatches = match._take == -1 ? 
                Math.Max(128, matches.Length) : // no limit specified, we'll guess on the size and rely on growing as needed 
                match._take;

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
            // We will allocate the space for the heap data structure that we are gonna use to fill the results.
            int sizeOfElement = Unsafe.SizeOf<MatchComparer<TComparer, TOut>.Item>();
            int heapSize = sizeOfElement * totalMatches;
            if (match._bufferSize < heapSize)
                match.UnlikelyGrowBuffer(heapSize);
            var heap = new SortingMatchHeap<TComparer, TOut>(match._comparer, match._buffer, totalMatches);
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
                            ref var matchesRef = ref MemoryMarshal.GetReference(matches);
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
                if (match._take == -1 && heap.CapacityIncreaseNeeded(read))
                {
                    // we don't have a limit to the number of results returned
                    // so we have to ensure that we keep *all* the results in memory, as such,
                    // we cannot limit the size of the sorting heap and need to grow it
                    match.UnlikelyGrowBuffer(match._bufferSize);
                    heap.IncreaseCapacity(match._buffer, match._bufferSize / sizeOfElement);
                }
            
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
                    
                    heap.Add(cur);
                }
            }
            bufferHandler.Dispose();

            if (matches.Length >= heap.Count)
            {
                heap.Complete(matches);
                match._bufferUsedCount = 0;
                return heap.Count;
            }

            // Note that we are *reusing* the same buffer that the heap is using, we can do that because the heap
            // will read from the end to the start, and write from the end as well. And it'll write sizeof(long) while
            // reading sizeof(Item), which is larger, so we are safe
            match._bufferUsedCount = heap.Count;
            Span<long> tempMatchesBuffer = GetTempMatchesBuffer(match._buffer, match._bufferSize, match._bufferUsedCount);
            heap.Complete(tempMatchesBuffer);

            ReturnMatches:

            if (match._bufferUsedCount == 0)
                return 0;
            
            var persistedMatches =  GetTempMatchesBuffer(match._buffer, match._bufferSize, match._bufferUsedCount);
            var matchesToReturn = Math.Min(persistedMatches.Length, matches.Length);
            match._bufferUsedCount -= matchesToReturn;
            persistedMatches[..matchesToReturn].CopyTo(matches);
            return matchesToReturn;
            
        }

        private static Span<long> GetTempMatchesBuffer(byte* buffer, int bufferSize, int bufferUsedCount)
        {
            return new Span<long>(buffer + bufferSize - bufferUsedCount * sizeof(long), bufferUsedCount);
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
