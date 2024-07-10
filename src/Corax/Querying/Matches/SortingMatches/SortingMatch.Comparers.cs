using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Corax.Mappings;
using Corax.Utils;
using Corax.Utils.Spatial;
using Sparrow;
using Sparrow.Server;
using Sparrow.Server.Utils.VxSort;
using Voron;
using Voron.Data.Containers;
using Voron.Data.Lookups;
using Voron.Impl;

namespace Corax.Querying.Matches.SortingMatches;

unsafe partial struct SortingMatch<TInner>
{
    private interface IEntryComparer
    {
        Slice GetSortFieldName(ref SortingMatch<TInner> match);
        void Init(ref SortingMatch<TInner> match);

        void SortBatch(ref SortingMatch<TInner> match, LowLevelTransaction llt, PageLocator pageLocator, Span<long> batchResults, Span<long> batchTermIds,
            UnmanagedSpan* batchTerms,
            bool descending = false);
    }
    
    private struct Descending<TInnerCmp> : IEntryComparer, IComparer<UnmanagedSpan>
        where TInnerCmp : struct, IEntryComparer, IComparer<UnmanagedSpan>
    {
        private TInnerCmp _cmp;

        public Descending(TInnerCmp cmp)
        {
            _cmp = cmp;
        }

        public Slice GetSortFieldName(ref SortingMatch<TInner> match)
        {
            return _cmp.GetSortFieldName(ref match);
        }

        public void Init(ref SortingMatch<TInner> match)
        {
            _cmp.Init(ref match);
        }

        public void SortBatch(ref SortingMatch<TInner> match, LowLevelTransaction llt, PageLocator pageLocator, Span<long> batchResults, Span<long> batchTermIds,
            UnmanagedSpan* batchTerms,
            bool descending = false)
        {
            _cmp.SortBatch(match: ref match, llt: llt, pageLocator: pageLocator, batchResults: batchResults, batchTermIds: batchTermIds, batchTerms: batchTerms,
                descending: true);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Compare(UnmanagedSpan x, UnmanagedSpan y)
        {
            return _cmp.Compare(y, x); // note the revered args
        }
    }

    private struct EntryComparerByScore : IEntryComparer, IComparer<UnmanagedSpan>
    {
        public Slice GetSortFieldName(ref SortingMatch<TInner> match)
        {
            throw new NotImplementedException("Scoring has no field name");
        }

        public void Init(ref SortingMatch<TInner> match)
        {
        }

        public void SortBatch(ref SortingMatch<TInner> match, LowLevelTransaction llt, PageLocator pageLocator, Span<long> batchResults, Span<long> batchTermIds,
            UnmanagedSpan* batchTerms,
            bool descending = false)
        {
            var readScores = MemoryMarshal.Cast<long, float>(batchTermIds)[..batchResults.Length];
            match._cancellationToken.ThrowIfCancellationRequested();

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
            var heapSize = Math.Min(match._take, batchResults.Length);
            heapSize = heapSize < 0 ? batchResults.Length : heapSize;
            var indexes = MemoryMarshal.Cast<long, int>(batchTermIds)[..(batchTermIds.Length)];
            using var _ = llt.Allocator.Allocate(heapSize, out Span<float> terms);
            var heapSorter = HeapSorterBuilder.BuildSingleNumericalSorter(indexes.Slice(0, heapSize), terms, descending == false);
            
            for (int i = 0; i < batchTermIds.Length; i++)
                heapSorter.Insert(i, readScores[i]);
            
            if (match._sortingDataTransfer.IncludeScores)
                heapSorter.FillWithTerms(batchResults, ref match._results, ref match._scoresResults);
            else
                heapSorter.Fill(batchResults, ref match._results);
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
            var match = Memory.Compare(xItem.Address + 1, yItem.Address + 1, Math.Min(xItem.Length - 1, yItem.Length - 1));
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
        private Lookup<Int64LookupKey> _lookup;

        public Slice GetSortFieldName(ref SortingMatch<TInner> match) => match._orderMetadata.Field.FieldName;

        public void Init(ref SortingMatch<TInner> match)
        {
            _lookup = match._searcher.EntriesToTermsReader(match._orderMetadata.Field.FieldName);
        }

        public void SortBatch(ref SortingMatch<TInner> match, LowLevelTransaction llt, PageLocator pageLocator, Span<long> batchResults, Span<long> batchTermIds,
            UnmanagedSpan* batchTerms,
            bool descending)
        {
            if (_lookup == null) // field does not exist, so arbitrary sort order, whatever query said goes
            {
                match._results.AddRange(batchResults);
                return;
            }

            match._cancellationToken.ThrowIfCancellationRequested();
            _lookup.GetFor(batchResults, batchTermIds, long.MinValue);
            Container.GetAll(llt, batchTermIds, batchTerms, long.MinValue, pageLocator);
            var indirectComparer = new IndirectComparer<CompactKeyComparer>(batchTerms, new CompactKeyComparer(), descending);
            var indexes = SortByTerms(ref match, batchTermIds, batchTerms, descending, indirectComparer);
            for (int i = 0; i < indexes.Length; i++)
            {
                int bIdx = indexes[i];
                match._results.Add(batchResults[bIdx]);
            }
        }

        private static void MaybeBreakTies<TComparer>(Span<long> buffer, TComparer tieBreaker, bool isDescending) where TComparer : struct, IComparer<long>
        {
            // We may have ties, have to resolve that before we can continue
            for (int i = 1; i < buffer.Length; i++)
            {
                var x = buffer[i - 1];
                var y = buffer[i];
                if (isDescending)
                {
                    x = -x;
                    y = -y;
                }

                x >>= 15;
                y >>= 15;
                if (x != y)
                    continue;

                // we have a match on the prefix, need to figure out where it ends hopefully this is rare
                int end = i;
                for (; end < buffer.Length; end++)
                {
                    y = buffer[end];
                    if (isDescending)
                        y = -y;
                    y >>= 15;
                    if (x != y)
                        break;
                }

                buffer[(i - 1)..end].Sort(tieBreaker);
                i = end;
            }
        }

        private static Span<int> SortByTerms<TComparer>(ref SortingMatch<TInner> match, Span<long> buffer, UnmanagedSpan* batchTerms, bool isDescending,
            TComparer tieBreaker)
            where TComparer : struct, IComparer<long>
        {
            Debug.Assert(buffer.Length < (1 << 15), "buffer.Length < (1<<15)");
            for (int i = 0; i < buffer.Length; i++)
            {
                long l = 0;
                if (batchTerms[i].Address != null)
                {
                    Memory.Copy(&l, batchTerms[i].Address + 1 /* skip metadata byte */,
                        Math.Min(6, batchTerms[i].Length - 1));
                }
                else
                {
                    l = -1 >>> 16; // effectively move to the end
                }

                l = BinaryPrimitives.ReverseEndianness(l) >>> 1;
                long sortKey = l | (uint)i;
                if (isDescending)
                    sortKey = -sortKey;
                buffer[i] = sortKey;
            }


            Sort.Run(buffer);

            if (match._take >= 0 &&
                buffer.Length > match._take)
                buffer = buffer[..match._take];

            MaybeBreakTies(buffer, tieBreaker, isDescending);

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
            return x.ToSpan().SequenceCompareTo(y.ToSpan());
            return _cmpTerm.Compare(x, y);
        }
    }


    private struct EntryComparerHelper
    {
        public static Span<int> NumericSortBatch<TCmp>(Span<long> batchTermIds, UnmanagedSpan* batchTerms, bool descending = false)
            where TCmp : struct, IComparer<UnmanagedSpan>, IEntryComparer
        {
            var indexes = MemoryMarshal.Cast<long, int>(batchTermIds)[..(batchTermIds.Length)];
            for (int i = 0; i < batchTermIds.Length; i++)
            {
                batchTerms[i] = new UnmanagedSpan(batchTermIds[i]);
                indexes[i] = i;
            }

            IndirectSort<TCmp>(indexes, batchTerms, descending);

            return indexes;
        }

        public static void IndirectSort<TCmp>(Span<int> indexes, UnmanagedSpan* batchTerms, bool descending, TCmp cmp = default)
            where TCmp : struct, IComparer<UnmanagedSpan>, IEntryComparer
        {
            if (descending)
            {
                indexes.Sort(new IndirectComparer<Descending<TCmp>>(batchTerms, new(cmp), true));
            }
            else
            {
                indexes.Sort(new IndirectComparer<TCmp>(batchTerms, cmp, false));
            }
        }
    }

    private struct EntryComparerByLong : IEntryComparer, IComparer<UnmanagedSpan>
    {
        private Lookup<Int64LookupKey> _lookup;

        public Slice GetSortFieldName(ref SortingMatch<TInner> match)
        {
            IndexFieldsMappingBuilder.GetFieldNameForLongs(match._searcher.Allocator, match._orderMetadata.Field.FieldName, out var lngName);
            return lngName;
        }


        public void Init(ref SortingMatch<TInner> match)
        {
            _lookup = match._searcher.EntriesToTermsReader(GetSortFieldName(ref match));
        }

        public void SortBatch(ref SortingMatch<TInner> match, LowLevelTransaction llt, PageLocator pageLocator, Span<long> batchResults, Span<long> batchTermIds,
            UnmanagedSpan* batchTerms,
            bool descending = false)
        {
            if (_lookup == null) // field does not exist, so arbitrary sort order, whatever query said goes
            {
                match._results.AddRange(batchResults);
                return;
            }

            _lookup.GetFor(batchResults, batchTermIds, long.MinValue);
            match._cancellationToken.ThrowIfCancellationRequested();
            var heapSize = Math.Min(match._take, batchResults.Length);
            heapSize = heapSize <= 0 ? batchResults.Length : heapSize;
            var indexes = MemoryMarshal.Cast<long, int>(batchTermIds)[..(batchTermIds.Length)];
            using var _ = llt.Allocator.Allocate(heapSize, out Span<long> terms);
            var sorter = HeapSorterBuilder.BuildSingleNumericalSorter(indexes.Slice(0, heapSize), terms, descending);

            for (var i = 0; i < batchResults.Length; ++i)
                sorter.Insert(i, batchTermIds[i]);
            
            sorter.Fill(batchResults, ref match._results);
        }

        public int Compare(UnmanagedSpan x, UnmanagedSpan y)
        {
            return x.Long.CompareTo(y.Long);
        }
    }

    private struct EntryComparerByDouble : IEntryComparer, IComparer<UnmanagedSpan>
    {
        private Lookup<Int64LookupKey> _lookup;

        public void SortBatch(ref SortingMatch<TInner> match, LowLevelTransaction llt, PageLocator pageLocator, Span<long> batchResults, Span<long> batchTermIds,
            UnmanagedSpan* batchTerms,
            bool descending = false)
        {
            if (_lookup == null) // field does not exist, so arbitrary sort order, whatever query said goes
            {
                match._results.AddRange(batchResults);
                return;
            }

            _lookup.GetFor(batchResults, batchTermIds, BitConverter.DoubleToInt64Bits(double.MinValue));
            match._cancellationToken.ThrowIfCancellationRequested();
            var heapSize = Math.Min(match._take, batchResults.Length);
            heapSize = heapSize <= 0 ? batchResults.Length : heapSize;
            var indexes = MemoryMarshal.Cast<long, int>(batchTermIds)[..(batchTermIds.Length)];
            using var _ = llt.Allocator.Allocate(heapSize, out Span<double> terms);
            var sorter = HeapSorterBuilder.BuildSingleNumericalSorter(indexes.Slice(0, heapSize), terms, descending);

            for (var i = 0; i < batchResults.Length; ++i)
                sorter.Insert(i, BitConverter.Int64BitsToDouble(batchTermIds[i]));
            
            sorter.Fill(batchResults, ref match._results);
        }

        public Slice GetSortFieldName(ref SortingMatch<TInner> match)
        {
            IndexFieldsMappingBuilder.GetFieldNameForDoubles(match._searcher.Allocator, match._orderMetadata.Field.FieldName, out var dblName);
            return dblName;
        }

        public void Init(ref SortingMatch<TInner> match)
        {
            _lookup = match._searcher.EntriesToTermsReader(GetSortFieldName(ref match));
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
        private Lookup<Int64LookupKey> _lookup;
        private int _take;
        private ByteStringContext _allocator;
        public Slice GetSortFieldName(ref SortingMatch<TInner> match) => match._orderMetadata.Field.FieldName;

        public void Init(ref SortingMatch<TInner> match)
        {
            _reader = match._searcher.TermsReaderFor(match._orderMetadata.Field.FieldName);
            _dictionaryId = match._searcher.GetDictionaryIdFor(match._orderMetadata.Field.FieldName);
            _lookup = match._searcher.EntriesToTermsReader(match._orderMetadata.Field.FieldName);
            _take = match._take;
            _allocator = match._searcher.Allocator;
        }

  
        
        public void SortBatch(ref SortingMatch<TInner> match, LowLevelTransaction llt, PageLocator pageLocator, Span<long> batchResults, Span<long> batchTermIds,
            UnmanagedSpan* batchTerms,
            bool descending = false)
        {
            if (_lookup == null) // field does not exist, so arbitrary sort order, whatever query said goes
            {
                match._results.AddRange(batchResults);
                return;
            }

            match._cancellationToken.ThrowIfCancellationRequested();
            _lookup.GetFor(batchResults, batchTermIds, long.MinValue);
            Container.GetAll(llt, batchTermIds, batchTerms, long.MinValue, pageLocator);
            var heapCapacity = _take == -1 ? batchResults.Length : Math.Min(_take, batchResults.Length);
            var documents = MemoryMarshal.Cast<long, int>(batchTermIds)[..(heapCapacity)];

            using var _ = _allocator.Allocate(heapCapacity, out Span<ByteString> terms);

            var heap = HeapSorterBuilder.BuildSingleAlphanumericalSorter(documents, terms, _allocator, descending);
            for (int i = 0; i < batchTermIds.Length; i++)
                heap.Insert(i, _reader.GetDecodedTerm(_dictionaryId, batchTerms[i]));

            heap.Fill(batchResults, ref match._results);
        }
        
        public int Compare(UnmanagedSpan x, UnmanagedSpan y)
        {
            throw new NotSupportedException($"{nameof(Compare)} in {nameof(EntryComparerByTermAlphaNumeric)} should never be used due to performance reasons.");
        }
    }

    private struct EntryComparerBySpatial : IEntryComparer, IComparer<UnmanagedSpan>
    {
        private SpatialReader _reader;
        private (double X, double Y) _center;
        private SpatialUnits _units;
        private double _round;

        public Slice GetSortFieldName(ref SortingMatch<TInner> match) => match._orderMetadata.Field.FieldName;

        public void Init(ref SortingMatch<TInner> match)
        {
            _center = (match._orderMetadata.Point.X, match._orderMetadata.Point.Y);
            _units = match._orderMetadata.Units;
            _round = match._orderMetadata.Round;
            _reader = match._searcher.SpatialReader(match._orderMetadata.Field.FieldName);
        }

        public void SortBatch(ref SortingMatch<TInner> match, LowLevelTransaction llt, PageLocator pageLocator, Span<long> batchResults, Span<long> batchTermIds,
            UnmanagedSpan* batchTerms,
            bool descending = false)
        {
            if (_reader.IsValid == false) // field does not exist, so arbitrary sort order, whatever query said goes
            {
                match._results.AddRange(batchResults);
                return;
            }

            var heapSize = Math.Min(match._take, batchResults.Length);
            heapSize = heapSize < 0 ? batchResults.Length : heapSize;
            
            var indexes = MemoryMarshal.Cast<long, int>(batchTermIds)[..(batchTermIds.Length)];
            var spatialResults = llt.Allocator.Allocate(heapSize, out Span<SpatialResult> terms);


            var heapSorter = HeapSorterBuilder.BuildSingleNumericalSorter<SpatialResult>(indexes.Slice(0, heapSize), terms, descending);
            


            for (int i = 0; i < batchResults.Length; i++)
            {
                SpatialResult distance; 
                if (_reader.TryGetSpatialPoint(batchResults[i], out var coords) == false)
                {
                    distance = new SpatialResult() { Distance = descending ? double.MinValue : double.MaxValue, Latitude = Double.NaN, Longitude = Double.NaN };
                }
                else
                {
                    distance = new SpatialResult()
                    {
                        Distance = SpatialUtils.GetGeoDistance(coords, _center, _round, _units), Longitude = coords.Lng, Latitude = coords.Lat
                    };
                }
                heapSorter.Insert(i, distance);
            }

            if (match._sortingDataTransfer.IncludeDistances)
                heapSorter.FillWithTerms(batchResults, ref match._results, ref match._distancesResults);
            else
                heapSorter.Fill(batchResults, ref match._results);
        }

        public int Compare(UnmanagedSpan x, UnmanagedSpan y)
        {
            return x.Double.CompareTo(y.Double);
        }
    }


    private readonly struct IndirectComparer<TComparer> : IComparer<long>, IComparer<int>
        where TComparer : struct, IComparer<UnmanagedSpan>
    {
        private readonly UnmanagedSpan* _terms;
        private readonly TComparer _inner;
        private readonly bool _isDescending;

        public IndirectComparer(UnmanagedSpan* terms, TComparer entryComparer, bool isDescending)
        {
            _terms = terms;
            _inner = entryComparer;
            _isDescending = isDescending;
        }

        public int Compare(long x, long y)
        {
            if (_isDescending)
            {
                x = -x;
                y = -y;
            }

            var xIdx = (ushort)x & 0X7FFF;
            var yIdx = (ushort)y & 0X7FFF;
            Debug.Assert(yIdx < SortingMatch.SortBatchSize && xIdx < SortingMatch.SortBatchSize);
            return _isDescending
                ? -_inner.Compare(_terms[xIdx], _terms[yIdx])
                : _inner.Compare(_terms[xIdx], _terms[yIdx]);
        }

        public int Compare(int x, int y)
        {
            return _inner.Compare(_terms[x], _terms[y]);
        }
    }
}
