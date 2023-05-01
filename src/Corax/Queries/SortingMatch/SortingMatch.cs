using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Corax.Mappings;
using Corax.Utils;
using Corax.Utils.Spatial;
using Sparrow.Server;

namespace Corax.Queries
{
    [DebuggerDisplay("{DebugView,nq}")]
    public unsafe struct SortingMatch<TInner> : IQueryMatch
        where TInner : IQueryMatch
    {
        private readonly IndexSearcher _searcher;
        private IQueryMatch _inner;
        private readonly OrderMetadata _orderMetadata;
        private readonly int _take;
        private readonly delegate*<ref SortingMatch<TInner>, Span<long>, int> _fillFunc;

        private const int NotStarted = -1;

        private byte* _entriesBuffer;
        private int _entriesCount;
        private int _bufferUsedCount;
        private ByteStringContext<ByteStringMemoryCache>.InternalScope _entriesBufferScope;

        public long TotalResults;

        public SortingMatch(IndexSearcher searcher, in TInner inner, OrderMetadata orderMetadata, int take = -1)
        {
            _searcher = searcher;
            _inner = inner;
            _orderMetadata = orderMetadata;
            _take = take;
            _bufferUsedCount = NotStarted;

            TotalResults = 0;

            if (_orderMetadata.HasBoost)
            {
                _fillFunc = SortBy<BoostingScorer, (long, float), EntryComparerByScore>(orderMetadata);
            }
            else
            {
                _fillFunc = _orderMetadata.FieldType switch
                {
                    MatchCompareFieldType.Sequence => SortBy<TermsScorer, long, EntryComparerByTerm>(orderMetadata),
                    MatchCompareFieldType.Alphanumeric => SortBy<TermsScorer, long, EntryComparerByTermAlphaNumeric>(orderMetadata),
                    // MatchCompareFieldType.Integer => &Fill<Fetcher, NumericalItem<long>>,
                    // MatchCompareFieldType.Floating => &Fill<Fetcher, NumericalItem<double>>,
                    MatchCompareFieldType.Spatial => SortBy<TermsScorer, long, EntryComparerBySpatial>(orderMetadata),
                    _ => throw new ArgumentOutOfRangeException(_orderMetadata.FieldType.ToString())
                };
            }
        }
        
        private interface IScorer<TItem>
            where TItem : unmanaged
        {
            void Init(ref SortingMatch<TInner> match, int length);
            void ComputeMatchScores(ref SortingMatch<TInner> match, Span<long> matches, int read);
            TItem GetItemFor(long entryId, int index);
        }

        private struct TermsScorer : IScorer<long>, IDisposable
        {
            public void Init(ref SortingMatch<TInner> match, int length)
            {
                
            }

            public void ComputeMatchScores(ref SortingMatch<TInner> match, Span<long> matches, int read)
            {
                
            }

            public long GetItemFor(long entryId, int index)
            {
                return entryId;
            }

            public void Dispose()
            {
                
            }
        }

        private struct BoostingScorer : IScorer<(long, float)>, IDisposable
        {
            private ByteStringContext<ByteStringMemoryCache>.InternalScope _bufferHandler;
            private float* _scoresBuffer;
            private int _length;

            public void Init(ref SortingMatch<TInner> match, int length)
            {
                _bufferHandler = match._searcher.Allocator.Allocate(length * sizeof(float), out var buffer);
                _scoresBuffer = (float*)buffer.Ptr;
                _length = length;
            }
            
            public void ComputeMatchScores(ref SortingMatch<TInner> match, Span<long> matches, int read)
            {
                Debug.Assert(_length == matches.Length);

                var readScores = new Span<float>(_scoresBuffer, read);

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
                        ref var scoresRef = ref MemoryMarshal.GetReference(readScores);
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

            public (long, float) GetItemFor(long entryId, int index)
            {
                return (entryId, _scoresBuffer[index]);
            }

            public EntryComparerByScore GetComparer(ref SortingMatch<TInner> match)
            {
                return new EntryComparerByScore();
            }

            public void Dispose()
            {
                _bufferHandler.Dispose();
            }
        }


        private static delegate*<ref SortingMatch<TInner>, Span<long>, int> SortBy<TScorer, TItem, TEntryComparer>(OrderMetadata metadata)
            where TScorer : struct, IScorer<TItem>, IDisposable
            where TItem : unmanaged
            where TEntryComparer : struct, IEntryComparer<TItem>, IComparerInit
        {
            if (metadata.Ascending)
            {
                return &Fill<TScorer, TItem, TEntryComparer>;
            }

            return &Fill<TScorer, TItem, Descending<TEntryComparer, TItem>>;
        }


        private static int Fill<TScorer,TItem, TEntryComparer>(ref SortingMatch<TInner> match, Span<long> matches)
            where TScorer : struct, IScorer<TItem>, IDisposable
            where TItem : unmanaged
            where TEntryComparer : struct, IEntryComparer<TItem>, IComparerInit
        {
            // This method should also be re-entrant for the case where we have already pre-sorted everything and 
            // we will just need to acquire via pages the totality of the results. 
            if (match._bufferUsedCount == NotStarted)
            {
                FillAndSortResults<TScorer, TItem,TEntryComparer>(ref match, matches);
                return match._bufferUsedCount;
            }

            if (match._entriesCount == match._bufferUsedCount)
            {
                match._entriesBufferScope.Dispose();
                return 0;
            }

            var persistedMatches = new Span<long>(match._entriesBuffer +  match._bufferUsedCount * sizeof(long), match._entriesCount - match._bufferUsedCount);
            var matchesToReturn = Math.Min(persistedMatches.Length, matches.Length);
            match._bufferUsedCount += matchesToReturn;
            persistedMatches[..matchesToReturn].CopyTo(matches);
            return matchesToReturn;
        }

        private struct Descending<TInnerCmp, TItem> : IEntryComparer<TItem>, IComparerInit
            where TInnerCmp : struct, IEntryComparer<TItem>, IComparerInit
            where TItem : unmanaged
        {
            private TInnerCmp cmp;

            public Descending()
            {
                cmp = new();
            }

            public int Compare(TItem x, TItem y)
            {
                return cmp.Compare(y, x); // note: reversed
            }

            public long GetEntryId(TItem x)
            {
                return cmp.GetEntryId(x);
            }

            public void Init(ref SortingMatch<TInner> match)
            {
                cmp.Init(ref match);
            }
        }

        private struct EntryComparerByScore : IEntryComparer<(long,float)>, IComparerInit
        {
            public int Compare((long, float) x, (long, float) y)
            {
                var cmp = x.Item2.CompareTo(y.Item2);
                if (cmp != 0) return cmp;
                return x.Item1.CompareTo(y.Item1);
            }

            public long GetEntryId((long, float) x)
            {
                return x.Item1;
            }

            public void Init(ref SortingMatch<TInner> match)
            {
                
            }
        }

        private interface IComparerInit
        {
            void Init(ref SortingMatch<TInner> match);
        }

        private  struct EntryComparerByTerm : IEntryComparer<long>, IComparerInit
        {
            private TermsReader _reader;

            public int Compare(long x, long y)
            {
                return _reader.Compare(x, y);
            }

            public long GetEntryId(long x)
            {
                return x;
            }

            public void Init(ref SortingMatch<TInner> match)
            {
                _reader = match._searcher.TermsReaderFor(match._orderMetadata.Field.FieldName);
            }
        }

        private struct EntryComparerByTermAlphaNumeric : IEntryComparer<long>, IComparerInit
        {
            private TermsReader _reader;

            public void Init(ref SortingMatch<TInner> match)
            {
                _reader = match._searcher.TermsReaderFor(match._orderMetadata.Field.FieldName);
            }

            public int Compare(long x, long y)
            {
                var hasX = _reader.TryGetTermFor(x, out ReadOnlySpan<byte> xTerm);
                var hasY = _reader.TryGetTermFor(y, out ReadOnlySpan<byte> yTerm);

                if (hasY == false)
                    return hasX ? 1 : 0;
                if (hasX == false)
                    return -1;

                return SortingMatch.BasicComparers.CompareAlphanumericAscending(xTerm, yTerm);
            }

            public long GetEntryId(long x)
            {
                return x;
            }
        }
        
        private struct EntryComparerBySpatial : IEntryComparer<long>, IComparerInit
        {
            private SpatialReader _reader;
            private (double X, double Y) _center;
            private SpatialUnits _units;
            private double _round;

            public void Init(ref SortingMatch<TInner> match)
            {
                _center = (match._orderMetadata.Point.X, match._orderMetadata.Point.Y);
                _units = match._orderMetadata.Units;
                _round = match._orderMetadata.Round;
                _reader = match._searcher.SpatialReader(match._orderMetadata.Field.FieldName);
            }

            public int Compare(long x, long y)
            {
                var hasX = _reader.TryGetSpatialPoint(x, out var xCoords);
                var hasY = _reader.TryGetSpatialPoint(y, out var yCoords);

                if (hasY == false)
                    return hasX ? 1 : 0;
                if (hasX == false)
                    return -1;

                var xDist = SpatialUtils.GetGeoDistance(xCoords, _center, _round, _units);
                var yDist = SpatialUtils.GetGeoDistance(yCoords, _center, _round, _units);

                return xDist.CompareTo(yDist);
            }

            public long GetEntryId(long x)
            {
                return x;
            }
        }

        private static void FillAndSortResults<TScorer, TItem, TEntryComparer>(ref SortingMatch<TInner> match, Span<long> matches) 
            where TScorer : struct, IScorer<TItem>, IDisposable
            where TItem : unmanaged
            where TEntryComparer : struct, IEntryComparer<TItem>, IComparerInit
        {
            TScorer scorer = new();
            Debug.Assert(matches.Length > 1);

            var bufferSize = match._take == -1
                ? Math.Max(128, matches.Length) // no limit specified, we'll guess on the size and rely on growing as needed
                : match._take;
            
            var bufferScope = match._searcher.Allocator.Allocate(bufferSize * sizeof(TItem), out ByteString bs);

            scorer.Init(ref match, matches.Length);

            // Initialize the important infrastructure for the sorting.
            TEntryComparer entryComparer = new();
            entryComparer.Init(ref match);
            var heap = new SortingMatchHeap<TEntryComparer, TItem>(entryComparer);
            heap.Set(bs);
            while (true)
            {
                var read = match._inner.Fill(matches);
                match.TotalResults += read;
                if (read == 0)
                    break;

                // Since we are doing boosting, we need to score the matches so we can use them later. 
                scorer.ComputeMatchScores(ref match, matches, read);

                // PERF: We perform this at the end to avoid this check if we are not really adding elements. 
                if (match._take == -1 && heap.CapacityIncreaseNeeded(read))
                {
                    // we don't have a limit to the number of results returned
                    // so we have to ensure that we keep *all* the results in memory, as such,
                    // we cannot limit the size of the sorting heap and need to grow it
                    match._searcher.Allocator.GrowAllocation(ref bs, ref bufferScope, bs.Length);
                    heap.Set(bs);
                }

                for (int i = 0; i < read; i++)
                {
                    var entry = scorer.GetItemFor(matches[i], i);
                    heap.Add(entry);
                }
            }

            match._entriesCount = heap.Count;
            match._bufferUsedCount = heap.Count;
            var matchesToUse = matches;
            if (heap.Count > matches.Length)
            {
                // we need to sort the values, we do that once, in a _new_ buffer that will be persisted for the next call
                // (note that we dispose the former buffer in the using above) and then we use this buffer till the end
                match._entriesBufferScope = match._searcher.Allocator.Allocate(heap.Count * sizeof(long), out bs);
                match._entriesBuffer = bs.Ptr;
                matchesToUse = new Span<long>(bs.Ptr, heap.Count);
            }
            
            heap.Complete(matchesToUse);

            if (matchesToUse.Length > matches.Length)
            {
                matchesToUse[..matches.Length].CopyTo(matches);
                match._bufferUsedCount = matchesToUse.Length;
            }

            scorer.Dispose();
            bufferScope.Dispose();   
        }

        public long Count => _inner.Count;

        public QueryCountConfidence Confidence => throw new NotSupportedException();

        public bool IsBoosting => _inner.IsBoosting || _orderMetadata.FieldType == MatchCompareFieldType.Score;

        public int AndWith(Span<long> buffer, int matches)
        {
            throw new NotSupportedException($"{nameof(SortingMatch<TInner>)} does not support the operation of {nameof(AndWith)}.");
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
            return new QueryInspectionNode($"{nameof(SortingMatch)} [{_orderMetadata}]",
                children: new List<QueryInspectionNode> { _inner.Inspect()},
                parameters: new Dictionary<string, string>()
                {
                        { nameof(IsBoosting), IsBoosting.ToString() },
                });
        }

        string DebugView => Inspect().ToString();
    }
}
