using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Corax.Mappings;
using Corax.Querying.Matches.Meta;
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

public unsafe partial struct SortingMultiMatch<TInner> : IQueryMatch
    where TInner : IQueryMatch
{
    private interface IEntryComparer : IComparer<int>, IComparer<UnmanagedSpan>
    {
        Slice GetSortFieldName(ref SortingMultiMatch<TInner> match);
        void Init(ref SortingMultiMatch<TInner> match, UnmanagedSpan<long> batchResults, int comparerId);

        void SortBatch<TComparer2, TComparer3>(ref SortingMultiMatch<TInner> match, LowLevelTransaction llt, PageLocator pageLocator,
            UnmanagedSpan<long> batchResults, Span<long> batchTermIds, UnmanagedSpan* batchTerms, OrderMetadata[] orderMetadata, TComparer2 comparer2,
            TComparer3 comparer3)
            where TComparer2 : struct, IComparer<UnmanagedSpan>, IComparer<int>, IEntryComparer
            where TComparer3 : struct, IComparer<UnmanagedSpan>, IComparer<int>, IEntryComparer;
    }
    
    private struct Descending<TInnerCmp> : IEntryComparer, IComparer<UnmanagedSpan>, IComparer<int>
        where TInnerCmp : struct, IEntryComparer, IComparer<UnmanagedSpan>, IComparer<int>
    {
        private TInnerCmp cmp;

        public Descending()
        {
            cmp = new();
        }
        
        public Descending(TInnerCmp cmp)
        {
            this.cmp = cmp;
        }

        public Slice GetSortFieldName(ref SortingMultiMatch<TInner> match)
        {
            return cmp.GetSortFieldName(ref match);
        }

        public void Init(ref SortingMultiMatch<TInner> match, UnmanagedSpan<long> batchResults, int comparerId)
        {
            cmp.Init(ref match, batchResults, comparerId);
        }

        public void SortBatch<TComparer2, TComparer3>(ref SortingMultiMatch<TInner> match, LowLevelTransaction llt, PageLocator pageLocator,
            UnmanagedSpan<long> batchResults, Span<long> batchTermIds, UnmanagedSpan* batchTerms, OrderMetadata[] orderMetadata, TComparer2 comparer2,
            TComparer3 comparer3)
            where TComparer2 : struct, IComparer<UnmanagedSpan>, IComparer<int>, IEntryComparer
            where TComparer3 : struct, IComparer<UnmanagedSpan>, IComparer<int>, IEntryComparer
        {
            cmp.SortBatch(ref match, llt,
                pageLocator, batchResults, batchTermIds, batchTerms, orderMetadata, comparer2, comparer3);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Compare(UnmanagedSpan x, UnmanagedSpan y)
        {
            return cmp.Compare(y, x); // note the revered args
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Compare(int x, int y) => cmp.Compare(y, x); // note the reversed args
    }
    
    private struct DescendingWrapper<TComparer> : IComparer<UnmanagedSpan>
        where TComparer : struct, IComparer<UnmanagedSpan>
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Compare(UnmanagedSpan x, UnmanagedSpan y)
        {
            return new TComparer().Compare(y, x); // note the reversed args
        }
    }

    private struct EntryComparerByScore : IEntryComparer, IComparer<UnmanagedSpan>, IComparer<int>
    {
        private UnmanagedSpan<float> _scores;
        private int _comparerId;
        public Slice GetSortFieldName(ref SortingMultiMatch<TInner> match)
        {
            throw new NotImplementedException("Scoring has no field name");
        }
        
        
        public void Init(ref SortingMultiMatch<TInner> match, UnmanagedSpan<long> batchResults, int comparerId)
        {
            if (comparerId == 0)
                return;

            _comparerId = comparerId;
            
            match._scoreBufferHandler = match._searcher.Allocator.Allocate(batchResults.Length * sizeof(float), out var scoreBuffer);
            _scores = new UnmanagedSpan<float>(scoreBuffer.Ptr, scoreBuffer.Length);
            match._secondaryScoreBuffer = _scores;
            
            var readScores = scoreBuffer.ToSpan<float>();
            
            readScores.Fill(Bm25Relevance.InitialScoreValue);
            match._inner.Score(batchResults, readScores, 1f);
            
            // If we need to do documents boosting then we need to modify the based on documents stored score. 
            if (match._searcher.DocumentsAreBoosted)
            {
                // We get the boosting tree and go to check every document. 
                BoostDocuments(match, batchResults, readScores);
            }
        }

        public void SortBatch<TComparer2, TComparer3>(ref SortingMultiMatch<TInner> match,
            LowLevelTransaction llt, PageLocator pageLocator, UnmanagedSpan<long> batchResults, Span<long> batchTermIds, UnmanagedSpan* batchTerms,
            OrderMetadata[] orderMetadata, TComparer2 comparer2, TComparer3 comparer3)
            where TComparer2 : struct, IComparer<UnmanagedSpan>, IComparer<int>, IEntryComparer
            where TComparer3 : struct, IComparer<UnmanagedSpan>, IComparer<int>, IEntryComparer
        {
            Debug.Assert(_comparerId == 0, "_comparerId == 0");
            
            var readScores = MemoryMarshal.Cast<long, float>(batchTermIds)[..batchResults.Length];

            // We have to initialize the score buffer with a positive number to ensure that multiplication (document-boosting) is taken into account when BM25 relevance returns 0 (for example, with AllEntriesMatch).
            readScores.Fill(Bm25Relevance.InitialScoreValue);

            // We perform the scoring process. 
            match._inner.Score(batchResults, readScores, 1f);
            match._token.ThrowIfCancellationRequested();

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

            match._token.ThrowIfCancellationRequested();
            EntryComparerHelper.IndirectSort<EntryComparerByScore, TComparer2, TComparer3>(ref match, indexes, batchTerms, new(), comparer2, comparer3);

            for (int i = 0; i < indexes.Length; i++)
            {
                match._results.Add(batchResults[indexes[i]]);
                if (match._sortingDataTransfer.IncludeScores)
                    match._scoresResults.Add((float)batchTerms[indexes[i]].Double);
            }
        }

        private static void BoostDocuments(SortingMultiMatch<TInner> match, Span<long> batchResults, Span<float> readScores)
        {
            var tree = match._searcher.GetDocumentBoostTree();
            if (tree is {NumberOfEntries: > 0})
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

        public int Compare(int x, int y)
        {
            Debug.Assert(_comparerId != 0, "_comparerId != 0");
            return _scores[y].CompareTo(_scores[x]);
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

    private struct EntryComparerByTerm : IEntryComparer, IComparer<UnmanagedSpan>, IComparer<int>
    {
        private CompactKeyComparer _cmpTerm;
        private Lookup<Int64LookupKey> _lookup;
        private UnmanagedSpan<long> _batchResults;
        private int _comparerId;
        private TermsReader _termsReader;
        public Slice GetSortFieldName(ref SortingMultiMatch<TInner> match) => match._orderMetadata[_comparerId].Field.FieldName;

        public void Init(ref SortingMultiMatch<TInner> match, UnmanagedSpan<long> batchResults, int comparerId)
        {
            _comparerId = comparerId;
            _lookup = match._searcher.EntriesToTermsReader(match._orderMetadata[_comparerId].Field.FieldName);
            _batchResults = batchResults;
            _termsReader = match._searcher.TermsReaderFor(match._orderMetadata[_comparerId].Field.FieldName);
        }

        public void SortBatch<TComparer2, TComparer3>(ref SortingMultiMatch<TInner> match, LowLevelTransaction llt, PageLocator pageLocator,
            UnmanagedSpan<long> batchResults, Span<long> batchTermIds, UnmanagedSpan* batchTerms, OrderMetadata[] orderMetadata, TComparer2 comparer2,
            TComparer3 comparer3)
            where TComparer2 : struct, IComparer<UnmanagedSpan>, IComparer<int>, IEntryComparer
            where TComparer3 : struct, IComparer<UnmanagedSpan>, IComparer<int>, IEntryComparer
        {
            if (_lookup == null) // field does not exist, so arbitrary sort order, whatever query said goes
            {
                match._results.AddRange(batchResults);
                return;
            }

            _lookup.GetFor(batchResults, batchTermIds, long.MinValue);
            Container.GetAll(llt, batchTermIds, batchTerms, long.MinValue, pageLocator);
            match._token.ThrowIfCancellationRequested();
            bool isDescending = orderMetadata[0].Ascending == false;
            object indirectComparer = isDescending ?
                new IndirectComparer<DescendingWrapper<CompactKeyComparer>, TComparer2, TComparer3>(ref match, batchTerms, new DescendingWrapper<CompactKeyComparer>(), comparer2, comparer3)
                : new IndirectComparer<CompactKeyComparer, TComparer2, TComparer3>(ref match, batchTerms, new CompactKeyComparer(), comparer2, comparer3);

            var indexes = isDescending
                ? SortByTerms(ref match, batchTermIds, batchTerms, (IndirectComparer<DescendingWrapper<CompactKeyComparer>, TComparer2, TComparer3>)indirectComparer)
                : SortByTerms(ref match, batchTermIds, batchTerms, (IndirectComparer<CompactKeyComparer, TComparer2, TComparer3>)indirectComparer);

            for (int i = 0; i < indexes.Length; i++)
            {
                int bIdx = indexes[i];
                match._results.Add(batchResults[bIdx]);
            }
        }

        private static void MaybeBreakTies<TComparer>(Span<long> buffer, TComparer tieBreaker) 
            where TComparer : struct, IComparer<long>
        {
            // We may have ties, have to resolve that before we can continue
            for (int i = 1; i < buffer.Length; i++)
            {
                var x = buffer[i - 1];
                var y = buffer[i];

                x >>= 15;
                y >>= 15;
                if (x != y)
                    continue;

                // we have a match on the prefix, need to figure out where it ends hopefully this is rare
                int end = i;
                for (; end < buffer.Length; end++)
                {
                    y = buffer[end];
                    y >>= 15;
                    if (x != y)
                        break;
                }

                buffer[(i - 1)..end].Sort(tieBreaker);
                i = end;
            }
        }

        private static Span<int> SortByTerms<TComparer>(ref SortingMultiMatch<TInner> match, Span<long> buffer, UnmanagedSpan* batchTerms,
            TComparer tieBreaker)
            where TComparer : struct, IComparer<long>, IComparer<int>
        {
            if (buffer.Length > SortingMatch.SortBatchSize)
            {
                return SortDirectly(buffer, tieBreaker);
            }

            Debug.Assert(buffer.Length < (1<<15),"buffer.Length < (1<<15)");
            
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
                
                buffer[i] = sortKey;
            }

            Sort.Run(buffer);
            MaybeBreakTies(buffer, tieBreaker);

            return ExtractIndexes(buffer);
        }
        
        private static Span<int> SortDirectly<TComparer>(Span<long> buffer, TComparer tieBreaker)
            where TComparer : struct, IComparer<int>
        {
            // note - we reuse the memory
            var indexes = MemoryMarshal.Cast<long, int>(buffer)[..(buffer.Length)];
            for (int i = 0; i < indexes.Length; i++)
            {
                indexes[i] = i;
            }
            indexes.Sort(tieBreaker);

            return indexes;
        }

        private static Span<int> ExtractIndexes(Span<long> buffer)
        {
            // note - we reuse the memory
            var indexes = MemoryMarshal.Cast<long, int>(buffer)[..(buffer.Length)];
            for (int i = 0; i < buffer.Length; i++)
            {
                var sortKey = buffer[i];
                var idx = (ushort)sortKey & 0x7FFF;
                indexes[i] = idx;
            }

            return indexes;
        }

        public int Compare(UnmanagedSpan x, UnmanagedSpan y)
        {
            return _cmpTerm.Compare(x, y);
        }

        public int Compare(int x, int y)
        {
            return _termsReader.Compare(_batchResults[x], _batchResults[y]);
        }
    }

    private struct EntryComparerByLong : IEntryComparer, IComparer<UnmanagedSpan>, IComparer<int>
    {
        private Lookup<Int64LookupKey> _lookup;
        private UnmanagedSpan<long> _batchResults;
        private int _comparerId;

        public Slice GetSortFieldName(ref SortingMultiMatch<TInner> match)
        {
            IndexFieldsMappingBuilder.GetFieldNameForLongs(match._searcher.Allocator, match._orderMetadata[_comparerId].Field.FieldName, out var lngName);
            return lngName;
        }

        public void Init(ref SortingMultiMatch<TInner> match, UnmanagedSpan<long> batchResults, int comparerId)
        {
            _comparerId = comparerId;
            _lookup = match._searcher.EntriesToTermsReader(GetSortFieldName(ref match));
            _batchResults = batchResults;
        }

        public void SortBatch<TComparer2, TComparer3>(ref SortingMultiMatch<TInner> match, LowLevelTransaction llt, PageLocator pageLocator,
            UnmanagedSpan<long> batchResults, Span<long> batchTermIds,
            UnmanagedSpan* batchTerms, OrderMetadata[] orderMetadata, TComparer2 comparer2, TComparer3 comparer3)
            where TComparer2 : struct, IComparer<UnmanagedSpan>, IComparer<int>, IEntryComparer
            where TComparer3 : struct, IComparer<UnmanagedSpan>, IComparer<int>, IEntryComparer
        {
            if (_lookup == null) // field does not exist, so arbitrary sort order, whatever query said goes
            {
                match._results.AddRange(batchResults);
                return;
            }

            _lookup.GetFor(batchResults, batchTermIds, long.MinValue);
            match._token.ThrowIfCancellationRequested();
            var indexes = EntryComparerHelper.NumericSortBatch(ref match, batchTermIds, batchTerms, new EntryComparerByLong(), comparer2, comparer3);
            for (int i = 0; i < indexes.Length; i++)
            {
                match._results.Add(batchResults[indexes[i]]);
            }
        }

        public int Compare(UnmanagedSpan x, UnmanagedSpan y)
        {
            return x.Long.CompareTo(y.Long);
        }

        public int Compare(int x, int y)
        {
            if (_lookup == null) // field does not exist, so arbitrary sort order, whatever query said goes
                return 0;

            Span<long> buffer = stackalloc long[4] {_batchResults[x], _batchResults[y], -1, -1};
            var swap = buffer[0] > buffer[1];
            if (swap)
                buffer[..2].Reverse();
            
            _lookup.GetFor(buffer[..2], buffer[2..], long.MinValue);
            if (swap) // In the case when we swapped the keys (since the lookup requires a sorted list as input), we have to swap the values before comparison to maintain the original order.
                buffer[2..].Reverse();
            
            return buffer[2].CompareTo(buffer[3]);
        }
    }

    private struct NullComparer : IEntryComparer, IComparer<UnmanagedSpan>
    {
        private const string NullComparerExceptionMessage = $"{nameof(NullComparer)} is for type-relaxation. You should not use it";
        public Slice GetSortFieldName(ref SortingMultiMatch<TInner> match)
        {
            throw new NotSupportedException(NullComparerExceptionMessage);
        }

        public void Init(ref SortingMultiMatch<TInner> match, UnmanagedSpan<long> batchResults, int comparerId)
        {
            //sometimes we can call init on this struct
        }

        public void SortBatch<TComparer2, TComparer3>(ref SortingMultiMatch<TInner> match, LowLevelTransaction llt, PageLocator pageLocator,
            UnmanagedSpan<long> batchResults, Span<long> batchTermIds,
            UnmanagedSpan* batchTerms, OrderMetadata[] orderMetadata, TComparer2 comparer2, TComparer3 comparer3)
            where TComparer2 : struct, IComparer<UnmanagedSpan>, IComparer<int>, IEntryComparer
            where TComparer3 : struct, IComparer<UnmanagedSpan>, IComparer<int>, IEntryComparer
        {
            throw new NotSupportedException(NullComparerExceptionMessage);
        }

        public int Compare(UnmanagedSpan x, UnmanagedSpan y)
        {
            throw new NotSupportedException(NullComparerExceptionMessage);
        }

        public int Compare(int x, int y)
        {
            throw new NotSupportedException(NullComparerExceptionMessage);
        }
    }

    private struct EntryComparerByDouble : IEntryComparer, IComparer<UnmanagedSpan>
    {
        private int _comparerId;
        private Lookup<Int64LookupKey> _lookup;
        private UnmanagedSpan<long> _batchResults;

        public void SortBatch<TComparer2, TComparer3>(ref SortingMultiMatch<TInner> match, LowLevelTransaction llt, PageLocator pageLocator,
            UnmanagedSpan<long> batchResults, Span<long> batchTermIds,
            UnmanagedSpan* batchTerms, OrderMetadata[] orderMetadata, TComparer2 comparer2, TComparer3 comparer3)
            where TComparer2 : struct, IComparer<UnmanagedSpan>, IComparer<int>, IEntryComparer
            where TComparer3 : struct, IComparer<UnmanagedSpan>, IComparer<int>, IEntryComparer
        {
            if (_lookup == null) // field does not exist, so arbitrary sort order, whatever query said goes
            {
                match._results.AddRange(batchResults);
                return;
            }

            _lookup.GetFor(batchResults, batchTermIds, BitConverter.DoubleToInt64Bits(double.MinValue));
            match._token.ThrowIfCancellationRequested();
            var indexes = EntryComparerHelper.NumericSortBatch(ref match, batchTermIds, batchTerms, new EntryComparerByDouble(), comparer2, comparer3);
            for (int i = 0; i < indexes.Length; i++)
            {
                match._results.Add(batchResults[indexes[i]]);
            }
        }

        public Slice GetSortFieldName(ref SortingMultiMatch<TInner> match)
        {
            IndexFieldsMappingBuilder.GetFieldNameForDoubles(match._searcher.Allocator, match._orderMetadata[_comparerId].Field.FieldName, out var dblName);
            return dblName;
        }

        public void Init(ref SortingMultiMatch<TInner> match, UnmanagedSpan<long> batchResults, int comparerId)
        {
            _comparerId = comparerId;
            _lookup = match._searcher.EntriesToTermsReader(GetSortFieldName(ref match));
            _batchResults = batchResults;
        }

        public int Compare(UnmanagedSpan x, UnmanagedSpan y)
        {
            return x.Double.CompareTo(y.Double);
        }

        public int Compare(int x, int y)
        {
            if (_lookup == null) // field does not exist, so arbitrary sort order, whatever query said goes
                return 0;

            var bufferPtr = stackalloc long[4] {_batchResults[x], _batchResults[y], -1, -1};
            var buffer = new Span<long>(bufferPtr, 4);
            var swap = buffer[0] > buffer[1];
            if (swap)
                buffer.Slice(0, 2).Reverse();
            
            _lookup.GetFor(buffer[..2], buffer[2..], BitConverter.DoubleToInt64Bits(double.MinValue));
            
            // In the case when we swapped the keys (since the lookup requires a sorted list as input), we have to swap the values before comparison to maintain the original order.
            if (swap)
                buffer.Slice(2,2).Reverse();
            
            var bufferPtrAsDouble = (double*)bufferPtr;
            return bufferPtrAsDouble[2].CompareTo(bufferPtrAsDouble[3]);
        }
    }

    private struct EntryComparerByTermAlphaNumeric : IEntryComparer, IComparer<UnmanagedSpan>, IComparer<int>
    {
        private TermsReader _reader;
        private long _dictionaryId;
        private Lookup<Int64LookupKey> _lookup;
        private UnmanagedSpan<long> _batchResults;
        private int _comparerId;

        public Slice GetSortFieldName(ref SortingMultiMatch<TInner> match) => match._orderMetadata[_comparerId].Field.FieldName;

        public void Init(ref SortingMultiMatch<TInner> match, UnmanagedSpan<long> batchResults, int comparerId)
        {
            _comparerId = comparerId;
            _reader = match._searcher.TermsReaderFor(match._orderMetadata[_comparerId].Field.FieldName);
            _dictionaryId = match._searcher.GetDictionaryIdFor(match._orderMetadata[_comparerId].Field.FieldName);
            _lookup = match._searcher.EntriesToTermsReader(match._orderMetadata[_comparerId].Field.FieldName);
            _batchResults = batchResults;
        }

        public void SortBatch<TComparer2, TComparer3>(ref SortingMultiMatch<TInner> match, LowLevelTransaction llt, PageLocator pageLocator,
            UnmanagedSpan<long> batchResults, Span<long> batchTermIds,
            UnmanagedSpan* batchTerms, OrderMetadata[] orderMetadata, TComparer2 comparer2, TComparer3 comparer3)
            where TComparer2 : struct, IComparer<UnmanagedSpan>, IComparer<int>, IEntryComparer
            where TComparer3 : struct, IComparer<UnmanagedSpan>, IComparer<int>, IEntryComparer
        {
            if (_lookup == null) // field does not exist, so arbitrary sort order, whatever query said goes
            {
                match._results.AddRange(batchResults);
                return;
            }

            _lookup.GetFor(batchResults, batchTermIds, long.MinValue);
            Container.GetAll(llt, batchTermIds, batchTerms, long.MinValue, pageLocator);
            var indexes = MemoryMarshal.Cast<long, int>(batchTermIds)[..(batchTermIds.Length)];
            for (int i = 0; i < batchTermIds.Length; i++)
            {
                indexes[i] = i;
            }
            
            match._token.ThrowIfCancellationRequested();
            EntryComparerHelper.IndirectSort(ref match, indexes, batchTerms, this, comparer2, comparer3);
            for (int i = 0; i < indexes.Length; i++)
            {
                match._results.Add(batchResults[indexes[i]]);
            }
        }


        public int Compare(UnmanagedSpan x, UnmanagedSpan y)
        {
            _reader.GetDecodedTerms(_dictionaryId, x, out var xTerm, y, out var yTerm);
            return BasicComparers.CompareAlphanumericAscending(xTerm, yTerm);
        }

        public int Compare(int x, int y)
        {
            return string.Compare(_reader.GetTermFor(_batchResults[x]), _reader.GetTermFor(_batchResults[y]), StringComparison.Ordinal);
        }
    }

    private struct EntryComparerBySpatial : IEntryComparer, IComparer<UnmanagedSpan>
    {
        private SpatialReader _reader;
        private (double X, double Y) _center;
        private SpatialUnits _units;
        private double _round;
        private int _comparerId;
        private UnmanagedSpan<long> _batchResults;
        
        public Slice GetSortFieldName(ref SortingMultiMatch<TInner> match) => match._orderMetadata[_comparerId].Field.FieldName;

        public void Init(ref SortingMultiMatch<TInner> match, UnmanagedSpan<long> batchResults, int comparerId)
        {
            _batchResults = batchResults;
            _comparerId = comparerId;
            _center = (match._orderMetadata[_comparerId].Point.X, match._orderMetadata[_comparerId].Point.Y);
            _units = match._orderMetadata[_comparerId].Units;
            _round = match._orderMetadata[_comparerId].Round;
            _reader = match._searcher.SpatialReader(match._orderMetadata[_comparerId].Field.FieldName);
        }

        public void SortBatch<TComparer2, TComparer3>(ref SortingMultiMatch<TInner> match, LowLevelTransaction llt, PageLocator pageLocator,
            UnmanagedSpan<long> batchResults, Span<long> batchTermIds,
            UnmanagedSpan* batchTerms, OrderMetadata[] orderMetadata, TComparer2 comparer2, TComparer3 comparer3)
            where TComparer2 : struct, IComparer<UnmanagedSpan>, IComparer<int>, IEntryComparer
            where TComparer3 : struct, IComparer<UnmanagedSpan>, IComparer<int>, IEntryComparer
        {
            match._token.ThrowIfCancellationRequested();

            if (_reader.IsValid == false) // field does not exist, so arbitrary sort order, whatever query said goes
            {
                match._results.AddRange(batchResults);
                return;
            }
            
            var spatialResults = match._sortingDataTransfer.IncludeDistances 
                ? new Span<SpatialResult>((byte*)batchTerms + batchResults.Length * sizeof(UnmanagedSpan), batchResults.Length)
                : Span<SpatialResult>.Empty;
            
            var indexes = MemoryMarshal.Cast<long, int>(batchTermIds)[..(batchTermIds.Length)];
            for (int i = 0; i < batchResults.Length; i++)
            {
                double distance;
                if (_reader.TryGetSpatialPoint(batchResults[i], out var coords) == false)
                {
                    if (spatialResults.Length > 0)
                        spatialResults[i] = SpatialResult.Invalid;
                    
                    // always at the bottom, then, desc & asc
                    distance = match._orderMetadata[0].Ascending == false ? double.MinValue : double.MaxValue;
                }
                else
                {
                    distance = SpatialUtils.GetGeoDistance(coords, _center, _round, _units);

                    if (spatialResults.Length > 0)
                        spatialResults[i] = new SpatialResult() {Distance = distance, Latitude = coords.Lat, Longitude = coords.Lng};
                }

                batchTerms[i] = new UnmanagedSpan(distance);
                indexes[i] = i;
            }
            
            
            match._token.ThrowIfCancellationRequested();
            EntryComparerHelper.IndirectSort<EntryComparerByDouble, TComparer2, TComparer3>(ref match, indexes, batchTerms, new(), comparer2, comparer3);

            
            
            for (int i = 0; i < indexes.Length; i++)
            {
                match._results.Add(batchResults[indexes[i]]);

                if (match._sortingDataTransfer.IncludeDistances)
                    match._distancesResults.Add(spatialResults[indexes[i]]);
            }
        }

        public int Compare(UnmanagedSpan x, UnmanagedSpan y)
        {
            return x.Double.CompareTo(y.Double);
        }

        public int Compare(int x, int y)
        {
            // always as asc, if comparer is desc it's wrapped into Descending<> and params are switched

            double xDistance =
                _reader.TryGetSpatialPoint(_batchResults[x], out var coords) == false 
                ? double.MaxValue 
                : SpatialUtils.GetGeoDistance(coords, _center, _round, _units);

            double yDistance = _reader.TryGetSpatialPoint(_batchResults[y], out coords) == false 
                ? double.MaxValue 
                : SpatialUtils.GetGeoDistance(coords, _center, _round, _units);

            return xDistance.CompareTo(yDistance);
        }
    }
    
    private readonly struct IndirectComparer<TComparer1, TComparer2, TComparer3> : IComparer<long>, IComparer<int>
        where TComparer1 : struct, IComparer<UnmanagedSpan>
        where TComparer2 : struct, IComparer<int>, IComparer<UnmanagedSpan>
        where TComparer3 : struct, IComparer<int>, IComparer<UnmanagedSpan>
    {
        private readonly UnmanagedSpan* _terms;
        private readonly TComparer1 _cmp1;
        private readonly TComparer2 _cmp2;
        private readonly TComparer3 _cmp3;
        private readonly IEntryComparer[] _nextComparers;
        private readonly int _maxDegreeOfInnerComparer;

        public IndirectComparer(ref SortingMultiMatch<TInner> match, UnmanagedSpan* terms, TComparer1 entryComparer, TComparer2 cmp2, TComparer3 cmp3)
        {
            _terms = terms;
            _cmp1 = entryComparer;
            _cmp2 = cmp2;
            _cmp3 = cmp3;
            _nextComparers = match._nextComparers;

            if (typeof(TComparer1) == typeof(NullComparer))
                _maxDegreeOfInnerComparer = 0;
            else if (typeof(TComparer2) == typeof(NullComparer))
                _maxDegreeOfInnerComparer = 1;
            else if (typeof(TComparer3) == typeof(NullComparer))
                _maxDegreeOfInnerComparer = 2;
            else
                _maxDegreeOfInnerComparer = 3;
            
            _maxDegreeOfInnerComparer += _nextComparers.Length;
        }

        public int Compare(long x, long y)
        {
            var xIdx = (ushort)x & 0X7FFF;
            var yIdx = (ushort)y & 0X7FFF;

            Debug.Assert(yIdx < SortingMatch.SortBatchSize && xIdx < SortingMatch.SortBatchSize);

            var cmp = 0;
            for (int comparerId = 0; cmp == 0 && comparerId < _maxDegreeOfInnerComparer; ++comparerId)
            {
                cmp = comparerId switch
                {
                    0 => _cmp1.Compare(_terms[xIdx], _terms[yIdx]),
                    1 => _cmp2.Compare(xIdx, yIdx),
                    2 => _cmp3.Compare(xIdx, yIdx),
                    _ => _nextComparers[comparerId - 3].Compare(xIdx, yIdx)
                };
            }
            
            return cmp;
        }


        public int Compare(int x, int y)
        {
            var cmp = 0;
            for (int comparerId = 0; cmp == 0 && comparerId < _maxDegreeOfInnerComparer; ++comparerId)
            {
                cmp = comparerId switch
                {
                    0 => _cmp1.Compare(_terms[x], _terms[y]),
                    1 => _cmp2.Compare(x, y),
                    2 => _cmp3.Compare(x, y),
                    _ => _nextComparers[comparerId - 3].Compare(x, y)
                };
            }

            return cmp;
        }
    }
}
