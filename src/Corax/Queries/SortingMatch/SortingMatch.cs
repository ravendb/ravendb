using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Reflection.Metadata;
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

        private long* _entriesBuffer;
        private float* _scoresBuffer;
        private int _bufferSize;
        private int _bufferUsedCount;
        private ByteStringContext<ByteStringMemoryCache>.InternalScope _entriesBufferScope;
        private ByteStringContext<ByteStringMemoryCache>.InternalScope _scoresBufferScope;

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
                // _fillFunc = &Fill<Fetcher, NumericalItem<float>>;
            }
            else
            {
                _fillFunc = _comparer.FieldType switch
                {
                    MatchCompareFieldType.Sequence or MatchCompareFieldType.Alphanumeric => &Fill,
                    // MatchCompareFieldType.Integer => &Fill<Fetcher, NumericalItem<long>>,
                    // MatchCompareFieldType.Floating => &Fill<Fetcher, NumericalItem<double>>,
                    // MatchCompareFieldType.Spatial => &Fill<Fetcher, NumericalItem<double>>,
                    // MatchCompareFieldType.Score => &Fill<Fetcher, NumericalItem<double>>,
                    _ => throw new ArgumentOutOfRangeException(_comparer.FieldType.ToString())
                };
            }
        }
        
        private static int Fill(ref SortingMatch<TInner, TComparer> match, Span<long> matches)
        {

            // This method should also be re-entrant for the case where we have already pre-sorted everything and 
            // we will just need to acquire via pages the totality of the results. 
            if (match._bufferUsedCount != NotStarted)
            {
                goto ReturnMatches;
            }
            
            ByteStringContext<ByteStringMemoryCache>.InternalScope bufferHandler = default;
            Debug.Assert(matches.Length > 1);

            match._bufferSize =  match._take == -1 ? 
                Math.Max(128, matches.Length) : // no limit specified, we'll guess on the size and rely on growing as needed 
                match._take;;
            match._entriesBufferScope = match._searcher.Allocator.Allocate(match._bufferSize * sizeof(long), out ByteString bs);
            match._entriesBuffer = (long*)bs.Ptr;

            Span<float> scores;
            if (typeof(TComparer) == typeof(BoostingComparer))
            {
                // Allocate the new buffer
                bufferHandler = match._searcher.Allocator.Allocate(matches.Length * sizeof(float), out var buffer);
                scores = new Span<float>(buffer.Ptr, matches.Length);
                
                match._scoresBufferScope = match._searcher.Allocator.Allocate(match._bufferSize * sizeof(float), out bs);
                match._scoresBuffer = (float*)bs.Ptr;
            }
            else
            {
                scores = Span<float>.Empty;
            }

            // Initialize the important infrastructure for the sorting.
            var termReader = match._searcher.TermsReaderFor(match._comparer.Field.FieldName);
            var heap = new SortingMatchHeap<TermsReader>(termReader);
            heap.Set(match._entriesBuffer, match._scoresBuffer, match._bufferSize);
            while (true)
            {
                var read = match._inner.Fill(matches);
                match.TotalResults += read;
                if (read == 0)
                    break;

                // Since we are doing boosting, we need to score the matches so we can use them later. 
                if (typeof(TComparer) == typeof(BoostingComparer))
                {
                    ComputeMatchScores(ref match, matches, scores, read);
                }

                // PERF: We perform this at the end to avoid this check if we are not really adding elements. 
                if (match._take == -1 && heap.CapacityIncreaseNeeded(read))
                {
                    // we don't have a limit to the number of results returned
                    // so we have to ensure that we keep *all* the results in memory, as such,
                    // we cannot limit the size of the sorting heap and need to grow it

                    IncreaseBufferSize(ref match, heap);
                    heap.Set(match._entriesBuffer, match._scoresBuffer, match._bufferSize);
                }
            
                for (int i = 0; i < read; i++)
                {
                    if (typeof(TComparer) == typeof(BoostingComparer))
                    {
                        heap.Add(matches[i], scores[i]);
                    }
                    else
                    {
                        heap.Add(matches[i], 0);
                    }
                }
            }
            bufferHandler.Dispose();

            using (match._scoresBufferScope)
            using (match._entriesBufferScope)
            {
                if (matches.Length >= heap.Count)
                {
                    heap.Complete(matches);
                    match._bufferUsedCount = 0;
                    return heap.Count;
                }

                match._bufferUsedCount = heap.Count;
                // we need to sort the values, we do that once, in a _new_ buffer that will be persisted for the next call
                // (note that we dispose the former buffer in the using above) and then we use this buffer till the end
                match._entriesBufferScope = match._searcher.Allocator.Allocate(heap.Count* sizeof(long), out bs);
                var tempMatches = new Span<long>(bs.Ptr, heap.Count);
                heap.Complete(tempMatches);
                match._entriesBuffer = (long*)bs.Ptr;
            }

            ReturnMatches:

            if (match._bufferUsedCount == 0)
            {
                match._entriesBufferScope.Dispose();
                return 0;
            }
            
            var persistedMatches = new Span<long>(match._entriesBuffer, match._bufferUsedCount);
            var matchesToReturn = Math.Min(persistedMatches.Length, matches.Length);
            match._bufferUsedCount -= matchesToReturn;
            match._entriesBuffer += matchesToReturn;
            persistedMatches[..matchesToReturn].CopyTo(matches);
            return matchesToReturn;
            
        }

        private static void IncreaseBufferSize(ref SortingMatch<TInner, TComparer> match, SortingMatchHeap<TermsReader> heap)
        {
            ByteString bs;
            var size = match._bufferSize * 2;

            var scope = match._searcher.Allocator.Allocate(size * sizeof(long), out bs);
            Memory.Copy(bs.Ptr, match._entriesBuffer, heap.Count * sizeof(long));
            match._entriesBufferScope.Dispose();
            match._entriesBufferScope = scope;
            match._entriesBuffer = (long*)bs.Ptr;

            if (typeof(TComparer) == typeof(BoostingComparer))
            {
                scope = match._searcher.Allocator.Allocate(size * sizeof(float), out bs);
                Memory.Copy(bs.Ptr, match._scoresBuffer, heap.Count * sizeof(float));
                match._entriesBufferScope.Dispose();
                match._scoresBufferScope = scope;
                match._scoresBuffer = (float*)bs.Ptr;
            }

            match._bufferSize = size;
        }

        private static void ComputeMatchScores(ref SortingMatch<TInner, TComparer> match, Span<long> matches, Span<float> scores, int read)
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
                    for (int idx = 0; idx < matches.Length; idx++)
                    {
                        var ptr = (float*)tree.ReadPtr(Unsafe.Add(ref matchesRef, idx), out var _);
                        if (ptr == null)
                            continue;

                        ref var scoresIdx = ref Unsafe.Add(ref scoresRef, idx);
                        scoresIdx *= *ptr;
                    }
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
