using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Corax.Utils;
using Corax.Utils.Spatial;
using Sparrow;
using Sparrow.Binary;
using Sparrow.Server;
using Sparrow.Server.Utils;
using Sparrow.Server.Utils.VxSort;
using Voron.Data.Containers;
using Voron.Data.Lookups;

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

        private struct Results : IDisposable
        {
            private long* _matches;
            private ByteString _matchesBuffer;
            private ByteString _termsBuffer;
            public int Count;
            private readonly ByteStringContext _allocator;
            private readonly int _max;
            private ByteStringContext<ByteStringMemoryCache>.InternalScope _matchesScope;
            private ByteStringContext<ByteStringMemoryCache>.InternalScope _termsScope;
            private int _capacity;
            private readonly UnmanagedSpan* _terms;

            public Results(ByteStringContext allocator, int max)
            {
                _allocator = allocator;
                _max = max;
                Count = NotStarted;

                _capacity = Math.Max(128, max);
                _matchesScope = allocator.Allocate(sizeof(long) * _capacity, out _matchesBuffer);
                _termsScope = allocator.Allocate(sizeof(UnmanagedSpan) * _capacity, out _termsBuffer);
                _matches = (long*)_matchesBuffer.Ptr;
                _terms = (UnmanagedSpan*)_termsBuffer.Ptr;
            }

            public int Prune<TEntryComparer>(TEntryComparer comparer, Span<UnmanagedSpan> unsortedTerms)
                  where TEntryComparer : struct, IComparer<UnmanagedSpan>
            {
                if (Count < _max)
                    return unsortedTerms.Length; // we cannot filter anything

                var pos = 0;
                var bottomTerm = _terms[Count - 1];
                for (int i = 0; i < unsortedTerms.Length; i++)
                {
                    if(comparer.Compare(bottomTerm, unsortedTerms[i]) > 0)
                    {
                        unsortedTerms[pos++] = unsortedTerms[i];
                    }
                }
                return pos;
            }

            public void Merge<TEntryComparer>(TEntryComparer comparer, Span<long> matches, Span<UnmanagedSpan> terms)
                where TEntryComparer : struct, IComparer<UnmanagedSpan>
            {
                Debug.Assert(matches.Length == terms.Length);
                
                if (_max == -1 || // no limit
                    Count + matches.Length < _max) // *after* the batch, we are still below the limit
                {
                    FullyMergeWith(comparer, matches, terms);
                    return;
                }
                 
                if(Count < _max)
                {
                    // fill up to maximum size
                    int sizeToFull = _max - Count;
                    FullyMergeWith(comparer, matches[..sizeToFull], terms[..sizeToFull]);
                    matches = matches[sizeToFull..];
                    terms = terms[sizeToFull..];
                }
                
                if(terms.Length > _max)
                {
                    terms = terms[.._max];
                }

                var b = terms.BinarySearch(_terms[Count - 1], comparer);
                if (b < 0)
                    b = ~b - 1;

                var d = Count - 1;
                while (b >= 0)
                {
                    int cmp = comparer.Compare(_terms[d], terms[b]);
                    if (cmp < 0)
                    {
                        _matches[d] = matches[b];
                        _terms[d] = terms[b];
                    }

                    d--;
                    b--;
                }
            }


            private void FullyMergeWith<TComparer>(TComparer comparer, Span<long> matches, Span<UnmanagedSpan> terms)
                where TComparer : struct, IComparer<UnmanagedSpan>
            {
                if (matches.Length + Count > _capacity)
                {
                    EnsureCapacity(matches.Length + Count );
                }

                var b = matches.Length - 1;
                var d = Count - 1;
                var k = b + Count;
                while (d >= 0 && b >= 0)
                {
                    int cmp = comparer.Compare(_terms[d], terms[b]);
                    if (cmp < 0)
                    {
                        _terms[k] = terms[b];
                        _matches[k] = matches[b];
                        b--;
                    }
                    else
                    {
                        d--;
                    }
                    k--;
                }
                matches[..(b+1)].CopyTo(new(_matches, _capacity));
                terms[..(b+1)].CopyTo(new(_terms, _capacity));
                Count += matches.Length;
            }
            private void EnsureCapacity(int req)
            {
                Debug.Assert(req > _capacity);

                var oldCapacity = _capacity;
                _capacity = (int)BitOperations.RoundUpToPowerOf2((uint)req);
                var additionalSize = _capacity - oldCapacity;
                _allocator.GrowAllocation(ref _matchesBuffer, ref _matchesScope, additionalSize * sizeof(long));
                _allocator.GrowAllocation(ref _termsBuffer, ref _termsScope, additionalSize * sizeof(UnmanagedSpan));
            }

            public void Dispose()
            {
                _termsScope.Dispose();
                _matchesScope.Dispose();
            }

            public int CopyTo(Span<long> matches)
            {
                var copy = Math.Min(matches.Length, Count);
                new Span<long>(_matches, copy).CopyTo(matches);
                _matches += copy;
                Count -= copy;
                return copy;
            }

            internal void Init()
            {
                Count = 0;
            }
        }
        
      
        private ByteStringContext<ByteStringMemoryCache>.InternalScope _entriesBufferScope;

        private Results _results;
        public long TotalResults;

        public SortingMatch(IndexSearcher searcher, in TInner inner, OrderMetadata orderMetadata, int take = -1)
        {
            _searcher = searcher;
            _inner = inner;
            _orderMetadata = orderMetadata;
            _take = take;
            _results = new Results(searcher.Allocator, take);

            TotalResults = 0;

            // if (_orderMetadata.HasBoost)
            // {
            //     _fillFunc = SortBy<BoostingScorer, (long, float), EntryComparerByScore>(orderMetadata);
            // }
            // else
            {
                _fillFunc = _orderMetadata.FieldType switch
                {
                    MatchCompareFieldType.Sequence => SortBy<TermsScorer, long, EntryComparerByTerm>(orderMetadata),
                    // MatchCompareFieldType.Alphanumeric => SortBy<TermsScorer, long, EntryComparerByTermAlphaNumeric>(orderMetadata),
                    // MatchCompareFieldType.Integer => SortBy<TermsScorer, long, EntryComparerByLong>(orderMetadata),
                    // MatchCompareFieldType.Floating => SortBy<TermsScorer, long, EntryComparerByDouble>(orderMetadata),
                    // MatchCompareFieldType.Spatial => SortBy<TermsScorer, long, EntryComparerBySpatial>(orderMetadata),
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
            where TEntryComparer : struct, IEntryComparer<TItem>, IComparerInit, IComparer<UnmanagedSpan>
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
            where TEntryComparer : struct, IEntryComparer<TItem>, IComparerInit, IComparer<UnmanagedSpan>
        {
            // This method should also be re-entrant for the case where we have already pre-sorted everything and 
            // we will just need to acquire via pages the totality of the results. 
            if (match._results.Count == NotStarted)
            {
                FillAndSortResults<TScorer, TItem,TEntryComparer>(ref match);
            }

            var read = match._results.CopyTo(matches);

            if (read != 0) 
                return read;
            
            match._results.Dispose();
            match._entriesBufferScope.Dispose();

            return 0;
        }

        private struct Descending<TInnerCmp, TItem> : IEntryComparer<TItem>, IComparerInit, IComparer<UnmanagedSpan> 
            where TInnerCmp : struct, IEntryComparer<TItem>, IComparerInit, IComparer<UnmanagedSpan>
            where TItem : unmanaged
        {
            private TInnerCmp cmp;

            public Descending()
            {
                cmp = new();
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public int Compare(TItem x, TItem y)
            {
                return cmp.Compare(y, x); // note: reversed
            }

            public long GetEntryId(TItem x)
            {
                return cmp.GetEntryId(x);
            }

            public string GetEntryText(TItem x)
            {
                return cmp.GetEntryText(x);
            }

            public Lookup<long> Init(ref SortingMatch<TInner> match)
            {
                return cmp.Init(ref match);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public int Compare(UnmanagedSpan x, UnmanagedSpan y)
            {
                return cmp.Compare(y, x); // note the revered args
            }
        }

        private struct EntryComparerByScore : IEntryComparer<(long,float)>, IComparerInit
        {
            public int Compare((long, float) x, (long, float) y)
            {
                // with order by score() we want to find the *highest* value by default, so we sort
                // in the opposite order by default for the values of the score
                var cmp = y.Item2.CompareTo(x.Item2);
                if (cmp != 0) return cmp;
                // if the score is identical, we then compare entry ids in the usual manner 
                return x.Item1.CompareTo(y.Item1);
            }

            public long GetEntryId((long, float) x)
            {
                return x.Item1;
            }

            public string GetEntryText((long, float) x)
            {
                return x.Item2.ToString(CultureInfo.InvariantCulture);
            }

            public Lookup<long> Init(ref SortingMatch<TInner> match)
            {
                return default;
            }
        }

        private interface IComparerInit
        {
            Lookup<long> Init(ref SortingMatch<TInner> match);
        }

        private struct EntryComparerByTerm : IEntryComparer<long>, IComparerInit, IComparer<UnmanagedSpan>
        {
            private TermsReader _reader;

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public int Compare(long x, long y)
            {
                var cmp = _reader.Compare(x, y);
                return cmp == 0 ? x.CompareTo(y) : cmp;
            }

            public long GetEntryId(long x)
            {
                return x;
            }

            public string GetEntryText(long x)
            {
                return _reader.GetTermFor(x);
            }

            public Lookup<long> Init(ref SortingMatch<TInner> match)
            {
                _reader = match._searcher.TermsReaderFor(match._orderMetadata.Field.FieldName);
                return match._searcher.TermsIdReaderFor(match._orderMetadata.Field.FieldName);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public int Compare(UnmanagedSpan xItem, UnmanagedSpan yItem)
            {
                if (yItem.Address == null)
                {
                    return xItem.Address == null ? 0 : 1;
                }

                if (xItem.Address == null)
                    return -1;
                var match = AdvMemory.Compare(xItem.Address + 1, yItem.Address + 1, Math.Min(xItem.Length - 1, yItem.Length - 1));
                if (match != 0)
                    return match;
                
                var xItemLengthInBits = (xItem.Length - 1) * 8 - (xItem.Address[0] >> 4);
                var yItemLengthInBits = (yItem.Length - 1) * 8 - (yItem.Address[0] >> 4);
                return xItemLengthInBits - yItemLengthInBits;
            }
        }
        
        private struct EntryComparerByLong : IEntryComparer<long>, IComparerInit
        {
            private Lookup<long> _lookup;

            public int Compare(long x, long y)
            {
                if (_lookup  == null)
                    return 0; // nothing to figure out _by_

                var hasX = _lookup.TryGetValue(x, out var xTerm);
                if (_lookup.TryGetValue(y, out var yTerm) == false)
                {
                    return hasX == false ? 0 : 1;
                }

                if (hasX == false)
                    return -1;

                var cmp = xTerm.CompareTo(yTerm);
                return cmp == 0 ? x.CompareTo(y) : cmp;
            }

            public long GetEntryId(long x)
            {
                return x;
            }

            public Lookup<long> Init(ref SortingMatch<TInner> match)
            {
                return _lookup = match._searcher.LongReader(match._orderMetadata.Field.FieldName);
            }
            
            public string GetEntryText(long x)
            {
                return _lookup.TryGetValue(x, out var xTerm) ? xTerm.ToString() : "n/a";
            }

        }
        
        private struct EntryComparerByDouble : IEntryComparer<long>, IComparerInit
        {
            private Lookup<long> _lookup;

            public int Compare(long x, long y)
            {
                if (_lookup  == null)
                    return 0; // nothing to figure out _by_

                var hasX = _lookup.TryGetValue(x, out var xTerm);
                if (_lookup.TryGetValue(y, out var yTerm) == false)
                {
                    return hasX == false ? 0 : 1;
                }

                if (hasX == false)
                    return -1;

                double xDouble = BitConverter.Int64BitsToDouble(xTerm);
                double yDouble = BitConverter.Int64BitsToDouble(yTerm);
                var cmp = xDouble.CompareTo(yDouble);
                return cmp == 0 ? x.CompareTo(y) : cmp;
            }

            public long GetEntryId(long x)
            {
                return x;
            }

            public string GetEntryText(long x)
            {
                return _lookup.TryGetValue(x, out var xTerm) ? xTerm.ToString() : "n/a";
            }

            public Lookup<long> Init(ref SortingMatch<TInner> match)
            {
                return _lookup = match._searcher.DoubleReader(match._orderMetadata.Field.FieldName);
            }
        }

        private struct EntryComparerByTermAlphaNumeric : IEntryComparer<long>, IComparerInit
        {
            private TermsReader _reader;

            public Lookup<long> Init(ref SortingMatch<TInner> match)
            {
                _reader = match._searcher.TermsReaderFor(match._orderMetadata.Field.FieldName);
                return match._searcher.TermsIdReaderFor(match._orderMetadata.Field.FieldName);

            }

            public int Compare(long x, long y)
            {
                _reader.GetDecodedTerms(x, out var xTerm, y, out var yTerm);

                var cmp = SortingMatch.BasicComparers.CompareAlphanumericAscending(xTerm, yTerm);
                return cmp == 0 ? x.CompareTo(y) : cmp;
            }

            public long GetEntryId(long x)
            {
                return x;
            }

            public string GetEntryText(long x)
            {
                return _reader.GetTermFor(x);
            }
        }
        
        private struct EntryComparerBySpatial : IEntryComparer<long>, IComparerInit
        {
            private SpatialReader _reader;
            private (double X, double Y) _center;
            private SpatialUnits _units;
            private double _round;

            public Lookup<long> Init(ref SortingMatch<TInner> match)
            {
                _center = (match._orderMetadata.Point.X, match._orderMetadata.Point.Y);
                _units = match._orderMetadata.Units;
                _round = match._orderMetadata.Round;
                _reader = match._searcher.SpatialReader(match._orderMetadata.Field.FieldName);
                return null;
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

                var cmp = xDist.CompareTo(yDist);
                return cmp == 0 ? x.CompareTo(y) : cmp;
            }

            public long GetEntryId(long x)
            {
                return x;
            }

            public string GetEntryText(long x)
            {
                _reader.TryGetSpatialPoint(x, out var xCoords);
                return xCoords.ToString();
            }
        }

        private const int SortBatchSize = 4096;
        
        private static void FillAndSortResults<TScorer, TItem, TEntryComparer>(ref SortingMatch<TInner> match) 
            where TScorer : struct, IScorer<TItem>, IDisposable
            where TItem : unmanaged
            where TEntryComparer : struct, IEntryComparer<TItem>, IComparerInit, IComparer<UnmanagedSpan>
        {
            var llt = match._searcher.Transaction.LowLevelTransaction;

            var allocator = match._searcher.Allocator;
            
            var matchesScope = allocator.Allocate( SortBatchSize * sizeof(long), out ByteString bs);
            Span<long> matches = new(bs.Ptr, SortBatchSize);
            var termsIdScope = allocator.Allocate(SortBatchSize * sizeof(long), out bs);
            Span<long> termIds = new(bs.Ptr, SortBatchSize);
            var termsScope = allocator.Allocate(SortBatchSize * sizeof(UnmanagedSpan), out bs);
            Span<UnmanagedSpan> terms = new(bs.Ptr, SortBatchSize);

            // Initialize the important infrastructure for the sorting.
            TEntryComparer entryComparer = new();
            var lookup = entryComparer.Init(ref match);
            match._results.Init();

            //var longSorters = default(Sorter<long, long, NumericComparer>);
            //var termsSorter = default(Sorter<UnmanagedSpan, long, TEntryComparer>);

            while (true)
            {
                var read = match._inner.Fill(matches);
                match.TotalResults += read;
                if (read == 0)
                    break;
                Span<long> batchResults = matches[..read];
                Span<long> batchTerms = termIds[..read];
                
                Sort.Run(batchResults);
                
                lookup.GetFor(batchResults, batchTerms, long.MinValue);

                batchTerms.Sort(batchResults);
                //longSorters.Sort(batchTerms, batchResults);
                
                Container.GetAll(llt, batchTerms, terms, long.MinValue);
                var curTerms = terms[..read];

                read = match._results.Prune(entryComparer, curTerms);
                if (read == 0)
                    continue;
                batchResults = batchResults[..read];
                curTerms = curTerms[..read];

                //termsSorter.Sort(curTerms, batchResults);
                curTerms.Sort(batchResults, entryComparer);
                
                match._results.Merge(entryComparer, batchResults, curTerms);
            }

            termsScope.Dispose();
            termsIdScope.Dispose();
            matchesScope.Dispose();
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
