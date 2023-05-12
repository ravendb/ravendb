using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Diagnostics;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Corax.Utils;
using Corax.Utils.Spatial;
using Sparrow;
using Sparrow.Server;
using Sparrow.Server.Utils.VxSort;
using Voron;
using Voron.Data.CompactTrees;
using Voron.Data.Containers;
using Voron.Data.Lookups;
using Voron.Impl;

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
            private readonly LowLevelTransaction _llt;
            private readonly ByteStringContext _allocator;
            private readonly int _max;
            private ByteStringContext<ByteStringMemoryCache>.InternalScope _matchesScope;
            private ByteStringContext<ByteStringMemoryCache>.InternalScope _termsScope;
            private int _capacity;
            private readonly UnmanagedSpan* _terms;

            public string[] DebugEntries => DebugTerms(_llt, new(_terms, Count));

            public Results(LowLevelTransaction llt, ByteStringContext allocator, int max)
            {
                _llt = llt;
                _allocator = allocator;
                _max = max;
                Count = NotStarted;

                _capacity = Math.Max(128, max);
                _matchesScope = allocator.Allocate(sizeof(long) * _capacity, out _matchesBuffer);
                _termsScope = allocator.Allocate(sizeof(UnmanagedSpan) * _capacity, out _termsBuffer);
                _matches = (long*)_matchesBuffer.Ptr;
                _terms = (UnmanagedSpan*)_termsBuffer.Ptr;
            }
            
            [MethodImpl(MethodImplOptions.AggressiveOptimization)]
            private static int BinarySearch<TEntryComparer>(TEntryComparer comparer,Span<int> indexes, Span<UnmanagedSpan> terms, UnmanagedSpan needle)
                where TEntryComparer : struct, IComparer<UnmanagedSpan>
            {
                int l = 0;
                int r = indexes.Length - 1;
                while (l <= r)
                {
                    var pivot = (l + r) >> 1;
                    var cmp = comparer.Compare(terms[indexes[pivot]], needle);
                    switch (cmp)
                    {
                        case 0:
                            return pivot;
                        case < 0:
                            l = pivot + 1;
                            break;
                        default:
                            r = pivot - 1;
                            break;
                    }
                }

                return ~l;
            }

            public void Merge<TEntryComparer>(TEntryComparer comparer,
                Span<int> indexes,
                Span<long> matches, 
                Span<UnmanagedSpan> terms)
                where TEntryComparer : struct, IComparer<UnmanagedSpan>
            {
                Debug.Assert(matches.Length == terms.Length);
                Debug.Assert(matches.Length == indexes.Length);

                if (_max == -1 || // no limit
                    Count + matches.Length < _max) // *after* the batch, we are still below the limit
                {
                    FullyMergeWith(comparer, indexes, matches, terms);
                    return;
                }
                 
                if(Count < _max)
                {
                    // fill up to maximum size
                    int sizeToFull = _max - Count;
                    FullyMergeWith(comparer, indexes[..sizeToFull], matches, terms);
                    indexes = indexes[sizeToFull..];
                }
                
                if(indexes.Length > _max)
                {
                    indexes = indexes[.._max];
                }

                if (indexes.Length == 0)
                    return;

                if (comparer.Compare(_terms[Count - 1], terms[indexes[0]]) < 0)
                {
                    // all the items in this batch are *larger* than the smallest item
                    // we keep, can skip it all
                    return;
                }

                if (comparer.Compare(_terms[0], terms[indexes[^1]]) > 0)
                {
                    // the first element we have is *larger* than the smallest
                    // element in the next batch, just copy it over and done
                    CopyToHeadOfBuffer(indexes, matches, terms, indexes.Length);
                    return;
                }

                for (int i = 0; i < Count; i++)
                {
                    var bIdx = indexes[0];
                    var cmp = comparer.Compare(_terms[i], terms[bIdx]);
                    if (cmp > 0)
                    {
                        Swap(_matches, i, matches, bIdx);
                        Swap(_terms, i, terms, bIdx);

                        // put it in the right place
                        int j = 1;
                        for (; j < indexes.Length; j++)
                        {
                            cmp = comparer.Compare(terms[indexes[j]], terms[bIdx]);
                            if (cmp >= 0)
                                break;

                            indexes[j - 1] = indexes[j];
                        }

                        indexes[j-1] = bIdx;
                    }
                }
            }

            private static void Swap<T>(T* a, int aIdx, Span<T> b, int bIdx) where T : unmanaged
            {
                (a[aIdx], b[bIdx]) = (b[bIdx], a[aIdx]);
            }
            


            private void FullyMergeWith<TComparer>(TComparer comparer, Span<int> indexes, Span<long> matches, Span<UnmanagedSpan> terms)
                where TComparer : struct, IComparer<UnmanagedSpan>
            {
                if (indexes.Length + Count > _capacity)
                {
                    EnsureCapacity(indexes.Length + Count);
                }

                var b = indexes.Length - 1;
                var d = Count - 1;
                var k = indexes.Length + Count - 1;
                while (d >= 0 && b >= 0)
                {
                    int bIdx = indexes[b];
                    int cmp = comparer.Compare(_terms[d], terms[bIdx]);
                    if (cmp < 0)
                    {
                        _terms[k] = terms[bIdx];
                        _matches[k] = matches[bIdx];
                        b--;
                    }
                    else
                    {
                        _terms[k] = _terms[d];
                        _matches[k] = _matches[d];
                        d--;
                    }
                  
                    k--;
                }

                CopyToHeadOfBuffer(indexes, matches, terms, b+1);
                Count += indexes.Length;
            }

            private void CopyToHeadOfBuffer(Span<int> indexes, Span<long> matches, Span<UnmanagedSpan> terms, int limit)
            {
                for (int i = 0; i < limit; i++)
                {
                    var bIdx = indexes[i];
                    _matches[i] = matches[bIdx];
                    _terms[i] = terms[bIdx];
                }
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
            _results = new Results(searcher.Transaction.LowLevelTransaction, searcher.Allocator, take);

            TotalResults = 0;

            if (_orderMetadata.HasBoost)
            {
                _fillFunc = SortBy<EntryComparerByScore>(orderMetadata);
            }
            else
            {
                _fillFunc = _orderMetadata.FieldType switch
                {
                    MatchCompareFieldType.Sequence => SortBy<EntryComparerByTerm>(orderMetadata),
                    MatchCompareFieldType.Alphanumeric => SortBy<EntryComparerByTermAlphaNumeric>(orderMetadata),
                    MatchCompareFieldType.Integer => SortBy<EntryComparerByLong>(orderMetadata),
                    MatchCompareFieldType.Floating => SortBy<EntryComparerByDouble>(orderMetadata),
                    MatchCompareFieldType.Spatial => SortBy<EntryComparerBySpatial>(orderMetadata),
                    _ => throw new ArgumentOutOfRangeException(_orderMetadata.FieldType.ToString())
                };
            }
        }
        
        
        private static delegate*<ref SortingMatch<TInner>, Span<long>, int> SortBy<TEntryComparer>(OrderMetadata metadata)
            where TEntryComparer : struct, IEntryComparer, IComparer<UnmanagedSpan>
        {
            if (metadata.Ascending)
            {
                return &Fill<TEntryComparer>;
            }

            return &Fill<Descending<TEntryComparer>>;
        }


        private static int Fill<TEntryComparer>(ref SortingMatch<TInner> match, Span<long> matches)
            where TEntryComparer : struct, IEntryComparer, IComparer<UnmanagedSpan>
        {
            // This method should also be re-entrant for the case where we have already pre-sorted everything and 
            // we will just need to acquire via pages the totality of the results. 
            if (match._results.Count == NotStarted)
            {
                FillAndSortResults<TEntryComparer>(ref match);
            }

            var read = match._results.CopyTo(matches);

            if (read != 0) 
                return read;
            
            match._results.Dispose();
            match._entriesBufferScope.Dispose();

            return 0;
        }

        private struct Descending<TInnerCmp> : IEntryComparer, IComparer<UnmanagedSpan> 
            where TInnerCmp : struct,  IEntryComparer, IComparer<UnmanagedSpan>
        {
            private TInnerCmp cmp;

            public Descending()
            {
                cmp = new();
            }

            public void Init(ref SortingMatch<TInner> match)
            {
                cmp.Init(ref match);
            }

            public Span<int> SortBatch(ref SortingMatch<TInner> match, LowLevelTransaction llt, PageLocator pageLocator, Span<long> batchResults, Span<long> batchTermIds,
                UnmanagedSpan* batchTerms,
                bool descending = false)
            {
                return cmp.SortBatch(match: ref match, llt: llt, pageLocator: pageLocator, batchResults: batchResults, batchTermIds: batchTermIds, batchTerms: batchTerms, descending: true);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public int Compare(UnmanagedSpan x, UnmanagedSpan y)
            {
                return cmp.Compare(y, x); // note the revered args
            }
        }

        private struct EntryComparerByScore : IEntryComparer, IComparer<UnmanagedSpan>
        {
            public void Init(ref SortingMatch<TInner> match)
            {
            }

            public Span<int> SortBatch(ref SortingMatch<TInner> match, LowLevelTransaction llt, PageLocator pageLocator, Span<long> batchResults, Span<long> batchTermIds,
                UnmanagedSpan* batchTerms,
                bool descending = false)
            {
                var readScores = MemoryMarshal.Cast<long, float>(batchTermIds)[..batchResults.Length];

                // We have to initialize the score buffer with a positive number to ensure that multiplication (document-boosting) is taken into account when BM25 relevance returns 0 (for example, with AllEntriesMatch).
                readScores.Fill(Bm25Relevance.InitialScoreValue);

                // We perform the scoring process. 
                match._inner.Score(batchResults, readScores, 1f);

                // If we need to do documents boosting then we need to modify the based on documents stored score. 
                if (match._searcher.DocumentsAreBoosted)
                {
                    // We get the boosting tree and go to check every document. 
                    BoostDocuments(match, batchResults, readScores);
                }
                
                // Note! readScores & indexes are aliased and same as batchTermIds
                var indexes = MemoryMarshal.Cast<long, int>(batchTermIds)[..(batchTermIds.Length)];
                for (int i = 0; i < batchTermIds.Length; i++)
                {
                    batchTerms[i] = new UnmanagedSpan(readScores[i]);
                    indexes[i] = i;
                }

                EntryComparerHelper.IndirectSort<EntryComparerByScore>(indexes, batchTerms, descending);
                
                return indexes;
            }

            private static void BoostDocuments(SortingMatch<TInner> match, Span<long> batchResults, Span<float> readScores)
            {
                var tree = match._searcher.GetDocumentBoostTree();
                if (tree is { NumberOfEntries: > 0 })
                {
                    // We are going to read from the boosting tree all the boosting values and apply that to the scores array.
                    ref var scoresRef = ref MemoryMarshal.GetReference(readScores);
                    ref var matchesRef = ref MemoryMarshal.GetReference(batchResults);
                    for (int idx = 0; idx < batchResults.Length; idx++)
                    {
                        var ptr = (float*)tree.ReadPtr(Unsafe.Add(ref matchesRef, idx), out var _);
                        if (ptr == null)
                            continue;

                        ref var scoresIdx = ref Unsafe.Add(ref scoresRef, idx);
                        scoresIdx *= *ptr;
                    }
                }
            }

            public int Compare(UnmanagedSpan x, UnmanagedSpan y)
            {
                // Note, for scores, we go *descending* by default!
                return y.Double.CompareTo(x.Double);
            }
        }

        private interface IEntryComparer
        {
            void Init(ref SortingMatch<TInner> match);

            Span<int> SortBatch(ref SortingMatch<TInner> match, LowLevelTransaction llt, PageLocator pageLocator, Span<long> batchResults, Span<long> batchTermIds,
                UnmanagedSpan* batchTerms,
                bool descending = false);
        }

        private struct CompactKeyComparer : IComparer<UnmanagedSpan>
        {
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

        private struct EntryComparerByTerm : IEntryComparer, IComparer<UnmanagedSpan>
        {
            private CompactKeyComparer _cmpTerm;
            private Lookup<long> _lookup;

            public void Init(ref SortingMatch<TInner> match)
            {
                 _lookup = match._searcher.TermsIdReaderFor(match._orderMetadata.Field.FieldName);
            }

            public Span<int> SortBatch(ref SortingMatch<TInner> match, LowLevelTransaction llt, PageLocator pageLocator, Span<long> batchResults, Span<long> batchTermIds,
                UnmanagedSpan* batchTerms,
                bool descending)
            {
                _lookup.GetFor(batchResults, batchTermIds, long.MinValue);
                Container.GetAll(llt, batchTermIds, batchTerms, long.MinValue, pageLocator);
                var indirectComparer = new IndirectComparer<CompactKeyComparer>(batchTerms, new CompactKeyComparer());
                return SortByTerms(batchTermIds, batchTerms, descending, indirectComparer);
            }

            private static void MaybeBreakTies<TComparer>(Span<long> buffer, TComparer tieBreaker) where TComparer : struct, IComparer<long>
            {
                // We may have ties, have to resolve that before we can continue
                for (int i = 1; i < buffer.Length; i++)
                {
                    var x = buffer[i - 1] >> 15;
                    var y = buffer[i] >> 15;
                    if (x != y)
                        continue;

                    // we have a match on the prefix, need to figure out where it ends hopefully this is rare
                    int end = i;
                    for (; end < buffer.Length; end++)
                    {
                        if (x != (buffer[end] >> 15))
                            break;
                    }

                    buffer[(i - 1)..end].Sort(tieBreaker);
                    i = end;
                }
            }

            private static long CopyTermPrefix(UnmanagedSpan item)
            {
                long l = 0;
                Memory.Copy(&l, item.Address + 1 /* skip metadata byte */, Math.Min(6, item.Length - 1));
                l = BinaryPrimitives.ReverseEndianness(l) >>> 1;
                return l;
            }

            private static Span<int> SortByTerms<TComparer>(Span<long> buffer, UnmanagedSpan* batchTerms, bool isDescending, TComparer tieBreaker)
                where TComparer : struct, IComparer<long>
            {
                for (int i = 0; i < buffer.Length; i++)
                {
                    long sortKey = CopyTermPrefix(batchTerms[i]) | (uint)i;
                    if (isDescending)
                        sortKey = -sortKey;
                    buffer[i] = sortKey;
                }


                Sort.Run(buffer);

                MaybeBreakTies(buffer, tieBreaker);

                return ExtractIndexes(buffer, isDescending);
            }

            private static Span<int> ExtractIndexes(Span<long> buffer, bool isDescending)
            {
                // note - we reuse the memory
                var indexes = MemoryMarshal.Cast<long, int>(buffer)[..(buffer.Length)];
                for (int i = 0; i < buffer.Length; i++)
                {
                    var sortKey = buffer[i];
                    if (isDescending)
                        sortKey = -sortKey;
                    var idx = (ushort)sortKey & 0x7FFF;
                    indexes[i] = idx;
                }

                return indexes;
            }

            public int Compare(UnmanagedSpan x, UnmanagedSpan y)
            {
                return _cmpTerm.Compare(x, y);
            }
        }

        
        private static string[] DebugTerms(LowLevelTransaction llt, Span<UnmanagedSpan> terms)
        {
            using var s = new CompactKeyCacheScope(llt);
            var l = new string[terms.Length];
            for (int i = 0; i < terms.Length; i++)
            {
                var item = terms[i];
                int remainderBits = item.Address[0] >> 4;
                int encodedKeyLengthInBits = (item.Length - 1) * 8 - remainderBits;
                long dicId = PersistentDictionary.CreateDefault(llt);
                s.Key.Set(encodedKeyLengthInBits, item.ToSpan()[1..], dicId);
                l[i] = s.Key.ToString();
            }

            return l;
        }

        private struct EntryComparerHelper
        {
            public static Span<int> NumericSortBatch<TCmp>(LowLevelTransaction llt, PageLocator pageLocator ,Span<long> batchTermIds, UnmanagedSpan* batchTerms, bool descending = false) 
                where TCmp : struct, IComparer<UnmanagedSpan>, IEntryComparer
            {
                Container.GetAll(llt, batchTermIds, batchTerms, long.MinValue, pageLocator);
                var indexes = MemoryMarshal.Cast<long, int>(batchTermIds)[..(batchTermIds.Length)];
                for (int i = 0; i < batchTermIds.Length; i++)
                {
                    indexes[i] = i;
                }

                IndirectSort<TCmp>(indexes, batchTerms, descending);
                
                return indexes;
            }

            public static void IndirectSort<TCmp>(Span<int> indexes, UnmanagedSpan* batchTerms, bool descending) 
                where TCmp : struct, IComparer<UnmanagedSpan>, IEntryComparer
            {
                if (descending)
                {
                    indexes.Sort(new IndirectComparer<Descending<TCmp>>(batchTerms, default));
                }
                else
                {
                    indexes.Sort(new IndirectComparer<TCmp>(batchTerms, default));
                }
            }
        }

        private struct EntryComparerByLong : IEntryComparer, IComparer<UnmanagedSpan>
        {
            private Lookup<long> _lookup;

            public void Init(ref SortingMatch<TInner> match)
            {
                _lookup = match._searcher.LongReader(match._orderMetadata.Field.FieldName);
            }

            public Span<int> SortBatch(ref SortingMatch<TInner> match, LowLevelTransaction llt, PageLocator pageLocator, Span<long> batchResults, Span<long> batchTermIds,
                UnmanagedSpan* batchTerms,
                bool descending = false)
            {
                _lookup.GetFor(batchResults, batchTermIds, long.MinValue);
                return EntryComparerHelper.NumericSortBatch<EntryComparerByLong>(llt, pageLocator, batchTermIds, batchTerms, descending);
            }

            public int Compare(UnmanagedSpan x, UnmanagedSpan y)
            {
                return x.Long.CompareTo(y.Long);
            }
        }
        
        private struct EntryComparerByDouble : IEntryComparer, IComparer<UnmanagedSpan>
        {
            private Lookup<long> _lookup;

            public Span<int> SortBatch(ref SortingMatch<TInner> match, LowLevelTransaction llt, PageLocator pageLocator, Span<long> batchResults, Span<long> batchTermIds,
                UnmanagedSpan* batchTerms,
                bool descending = false)
            {                _lookup.GetFor(batchResults, batchTermIds, BitConverter.DoubleToInt64Bits(double.MinValue));

                return EntryComparerHelper.NumericSortBatch<EntryComparerByDouble>(llt, pageLocator, batchTermIds, batchTerms, descending);
            }
            public void Init(ref SortingMatch<TInner> match)
            {
                _lookup = match._searcher.DoubleReader(match._orderMetadata.Field.FieldName);
            }

            public int Compare(UnmanagedSpan x, UnmanagedSpan y)
            {
                return x.Double.CompareTo(y.Double);
            }

        }

        private struct EntryComparerByTermAlphaNumeric : IEntryComparer, IComparer<UnmanagedSpan>
        {
            private TermsReader _reader;
            private long _dictionaryId;
            private Lookup<long> _lookup;

            public void Init(ref SortingMatch<TInner> match)
            {
                _reader = match._searcher.TermsReaderFor(match._orderMetadata.Field.FieldName);
                _dictionaryId = match._searcher.GetDictionaryIdFor(match._orderMetadata.Field.FieldName);
                _lookup = match._searcher.TermsIdReaderFor(match._orderMetadata.Field.FieldName);
            }

            public Span<int> SortBatch(ref SortingMatch<TInner> match, LowLevelTransaction llt, PageLocator pageLocator, Span<long> batchResults, Span<long> batchTermIds,
                UnmanagedSpan* batchTerms,
                bool descending = false)
            {
                _lookup.GetFor(batchResults, batchTermIds, long.MinValue);
                Container.GetAll(llt, batchTermIds, batchTerms, long.MinValue, pageLocator);
                var indexes = MemoryMarshal.Cast<long, int>(batchTermIds)[..(batchTermIds.Length)];
                for (int i = 0; i < batchTermIds.Length; i++)
                {
                    indexes[i] = i;
                }
                EntryComparerHelper.IndirectSort<EntryComparerByTermAlphaNumeric>(indexes, batchTerms, descending);
                return indexes;
            }


            public int Compare(UnmanagedSpan x, UnmanagedSpan y)
            {
                _reader.GetDecodedTerms(_dictionaryId, x, out var xTerm, y, out var yTerm);
                return SortingMatch.BasicComparers.CompareAlphanumericAscending(xTerm, yTerm);
            }
        }
        
        private struct EntryComparerBySpatial : IEntryComparer, IComparer<UnmanagedSpan>
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

            public Span<int> SortBatch(ref SortingMatch<TInner> match, LowLevelTransaction llt, PageLocator pageLocator, Span<long> batchResults, Span<long> batchTermIds,
                UnmanagedSpan* batchTerms,
                bool descending = false)
            {
                var indexes = MemoryMarshal.Cast<long, int>(batchTermIds)[..(batchTermIds.Length)];
                for (int i = 0; i < batchResults.Length; i++)
                {
                     double distance;
                    if (_reader.TryGetSpatialPoint(batchResults[i], out var coords) == false)
                    {
                        // always at the bottom, then, desc & asc
                        distance = descending ? double.MinValue : double.MaxValue;
                    }
                    else
                    {
                        distance = SpatialUtils.GetGeoDistance(coords, _center, _round, _units);
                    }

                    batchTerms[i] = new UnmanagedSpan(distance);
                    indexes[i] = i;
                }

                EntryComparerHelper.IndirectSort<EntryComparerByDouble>(indexes, batchTerms, descending);
                return indexes;
            }

            public int Compare(UnmanagedSpan x, UnmanagedSpan y)
            {
                return x.Double.CompareTo(y.Double);
            }
        }

        private const int SortBatchSize = 4096;

        private readonly struct IndirectComparer<TComparer> : IComparer<long>, IComparer<int>
            where TComparer : struct, IComparer<UnmanagedSpan>
        {
            private readonly UnmanagedSpan* _terms;
            private readonly TComparer _inner;

            public IndirectComparer(UnmanagedSpan* terms, TComparer entryComparer)
            {
                _terms = terms;
                _inner = entryComparer;
            }

            public int Compare(long x, long y)
            {
                var xIdx = (ushort)x & 0X7FFF;
                var yIdx = (ushort)y & 0X7FFF;
                Debug.Assert(yIdx < SortBatchSize && xIdx < SortBatchSize);
                return _inner.Compare(_terms[xIdx], _terms[yIdx]);
            }

            public int Compare(int x, int y)
            {
                return _inner.Compare(_terms[x], _terms[y]);
            }
        }

        private static void FillAndSortResults<TEntryComparer>(ref SortingMatch<TInner> match) 
            where TEntryComparer : struct,  IEntryComparer, IComparer<UnmanagedSpan>
        {
            var llt = match._searcher.Transaction.LowLevelTransaction;
            var allocator = match._searcher.Allocator;

            var matchesScope = allocator.Allocate(SortBatchSize * sizeof(long), out ByteString bs);
            Span<long> matches = new(bs.Ptr, SortBatchSize);
            var termsIdScope = allocator.Allocate(SortBatchSize * sizeof(long), out bs);
            Span<long> termIds = new(bs.Ptr, SortBatchSize);
            var termsScope = allocator.Allocate(SortBatchSize * sizeof(UnmanagedSpan), out bs);
            Span<UnmanagedSpan> terms = new(bs.Ptr, SortBatchSize);
            UnmanagedSpan* termsPtr = (UnmanagedSpan*)bs.Ptr;

            // Initialize the important infrastructure for the sorting.
            TEntryComparer entryComparer = new();
            entryComparer.Init(ref match);
            match._results.Init();
            
            var pageCache = new PageLocator(llt, 1024);

            while (true)
            {
                var read = match._inner.Fill(matches);
                match.TotalResults += read;
                if (read == 0)
                    break;

                var batchResults = matches[..read];
                var batchTermIds = termIds[..read];
                var batchTerms = terms[..read];

                Sort.Run(batchResults);

                Span<int> indexes = entryComparer.SortBatch(ref match, llt, pageCache, batchResults, batchTermIds, termsPtr);

                match._results.Merge(entryComparer, indexes, batchResults, batchTerms);
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
