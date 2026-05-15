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
using Voron.Util;

namespace Corax.Querying.Matches.SortingMatches;

public unsafe partial struct SortingMultiMatch<TInner> : IQueryMatch
    where TInner : IQueryMatch
{
    public DuplicatesOccurrence DuplicatesOccurrenceStatus => DuplicatesOccurrence.NotPossible;
    
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

    private struct EntryComparerByTerm : IEntryComparer, IComparer<UnmanagedSpan>, IComparer<int>
    {
        private Lookup<Int64LookupKey> _lookup;
        private UnmanagedSpan<long> _batchResults;
        private int _comparerId;
        private TermsReader _termsReader;
        private int _nullResult;
        private long _nullTermContainerId;
        private long _nonExistingTermContainerId;
        
        public Slice GetSortFieldName(ref SortingMultiMatch<TInner> match) => match._orderMetadata[_comparerId].Field.FieldName;

        
        public void Init(ref SortingMultiMatch<TInner> match, UnmanagedSpan<long> batchResults, int comparerId)
        {
            _comparerId = comparerId;
            var fieldName = match._orderMetadata[_comparerId].Field.FieldName;
            _lookup = match._searcher.EntriesToTermsReader(fieldName);
            _batchResults = batchResults;
            _termsReader = match._searcher.TermsReaderFor(fieldName);
            _nullResult = match.GetNullIsSmallestForClause(_comparerId) ? 1 : -1;
            
            if (match._searcher.TryGetPostingListForNull(fieldName, out _, out _nullTermContainerId) == false)
                _nullTermContainerId = -1;
            if (match._searcher.TryGetPostingListForNonExisting(fieldName, out _, out _nonExistingTermContainerId) == false)
                _nonExistingTermContainerId = -1;
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
            
            _lookup.GetFor(batchResults, batchTermIds, SortingHelpers.MissingTermId);
            SortingHelpers.ReplaceNullAndNonExistingTermIds(batchTermIds, _nonExistingTermContainerId, _nullTermContainerId, SortingHelpers.MissingTermId);
            Container.GetAll(llt, batchTermIds, new Span<UnmanagedSpan>(batchTerms, batchTermIds.Length), SortingHelpers.MissingTermId, pageLocator);
            match._token.ThrowIfCancellationRequested();

            var heapSize = Math.Min(match._take, batchResults.Length);
            heapSize = heapSize < 0 ? batchResults.Length : heapSize;
            var indexes = MemoryMarshal.Cast<long, int>(batchTermIds)[..(batchTermIds.Length)];
            var secondaryComparer = new IndirectComparer2<TComparer2, TComparer3>(ref match, comparer2, comparer3);
            using var _ = llt.Allocator.Allocate(heapSize, out Span<UnmanagedSpan> terms);
            var sorter = HeapSorterBuilder.BuildCompoundCompactKeySorter(indexes.Slice(0, heapSize), terms, orderMetadata[0].Ascending == false, secondaryComparer, match.GetNullIsSmallestForClause(_comparerId));
           
            for (int i = 0; i < indexes.Length; i++)
                sorter.Insert(i, batchTerms[i]);
            
            sorter.Fill(batchResults, ref match._results, ref match._scoresResults, match._secondaryScoreBuffer);
        }

        public int Compare(UnmanagedSpan x, UnmanagedSpan y)
        {
            return CompactKeyComparer.Compare(x, y, _nullResult);
        }

        public int Compare(int x, int y)
        {
            var termX = _termsReader.GetTerm(_batchResults[x], nullTermId: _nullTermContainerId, nonExistingTermId: _nonExistingTermContainerId);
            var termY = _termsReader.GetTerm(_batchResults[y], nullTermId: _nullTermContainerId, nonExistingTermId: _nonExistingTermContainerId);
            
            return CompactKeyComparer.Compare(termX, termY, _nullResult);
        }
    }

    private struct EntryComparerByLong : IEntryComparer, IComparer<UnmanagedSpan>, IComparer<int>
    {
        private Lookup<Int64LookupKey> _lookup;
        private UnmanagedSpan<long> _batchResults;
        private int _comparerId;
        private long _missingValue;
        
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
            _missingValue = match.GetNullIsSmallestForClause(_comparerId) ? long.MinValue : long.MaxValue;
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
            // load terms for documents
            _lookup.GetFor(batchResults, batchTermIds, _missingValue);

            var heapSize = Math.Min(match._take, batchResults.Length);
            heapSize = heapSize < 0 ? batchResults.Length : heapSize;
            
            using var _ = llt.Allocator.Allocate(heapSize, out Span<long> terms);
            var indexes = MemoryMarshal.Cast<long, int>(batchTermIds)[..(batchTermIds.Length)];
            var secondaryComparer = new IndirectComparer2<TComparer2, TComparer3>(ref match, comparer2, comparer3);
            var heapSorter = HeapSorterBuilder.BuildCompoundNumericalSorter(indexes.Slice(0, heapSize), terms, orderMetadata[0].Ascending == false, secondaryComparer, match.GetNullIsSmallestForClause(_comparerId));
                
            for (int i = 0; i < indexes.Length; i++)
                heapSorter.Insert(i, batchTermIds[i]);

            heapSorter.Fill(batchResults, ref match._results, ref match._scoresResults, match._secondaryScoreBuffer);
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
            
            _lookup.GetFor(buffer[..2], buffer[2..], _missingValue);
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
        private long _missingValue;
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
            // load terms for documents
            
            _lookup.GetFor(batchResults, batchTermIds, _missingValue);

            var heapSize = Math.Min(match._take, batchResults.Length);
            heapSize = heapSize < 0 ? batchResults.Length : heapSize;
            
            using var _ = llt.Allocator.Allocate(heapSize, out Span<double> terms);
            var indexes = MemoryMarshal.Cast<long, int>(batchTermIds)[..(batchTermIds.Length)];
            var secondaryComparer = new IndirectComparer2<TComparer2, TComparer3>(ref match, comparer2, comparer3);
            var heapSorter = HeapSorterBuilder.BuildCompoundNumericalSorter(indexes.Slice(0, heapSize), terms, orderMetadata[0].Ascending == false, secondaryComparer, match.GetNullIsSmallestForClause(_comparerId));
                
            for (int i = 0; i < indexes.Length; i++)
                heapSorter.Insert(i, BitConverter.Int64BitsToDouble(batchTermIds[i]));

            heapSorter.Fill(batchResults, ref match._results, ref match._scoresResults, match._secondaryScoreBuffer);
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
            _missingValue = BitConverter.DoubleToInt64Bits(match.GetNullIsSmallestForClause(_comparerId) ? double.MinValue : double.MaxValue);
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
            
            _lookup.GetFor(buffer[..2], buffer[2..], _missingValue);
            
            // In the case when we swapped the keys (since the lookup requires a sorted list as input), we have to swap the values before comparison to maintain the original order.
            if (swap)
                buffer.Slice(2,2).Reverse();
            
            var bufferPtrAsDouble = (double*)bufferPtr;
            return BitConverter.Int64BitsToDouble(buffer[2]).CompareTo(BitConverter.Int64BitsToDouble(buffer[3]));
        }
    }

    private struct EntryComparerByTermAlphaNumeric : IEntryComparer, IComparer<UnmanagedSpan>, IComparer<int>
    {
        private TermsReader _reader;
        private long _dictionaryId;
        private Lookup<Int64LookupKey> _lookup;
        private UnmanagedSpan<long> _batchResults;
        private int _comparerId;
        private ByteStringContext _allocator;
        private long _nullTermContainerId;
        private long _nonExistingTermContainerId;
        private bool _nullIsSmallest;

        public Slice GetSortFieldName(ref SortingMultiMatch<TInner> match) => match._orderMetadata[_comparerId].Field.FieldName;

        public void Init(ref SortingMultiMatch<TInner> match, UnmanagedSpan<long> batchResults, int comparerId)
        {
            _comparerId = comparerId;
            var fieldName = match._orderMetadata[_comparerId].Field.FieldName;
            _reader = match._searcher.TermsReaderFor(fieldName);
            _dictionaryId = match._searcher.GetDictionaryIdFor(fieldName);
            _lookup = match._searcher.EntriesToTermsReader(fieldName);
            _batchResults = batchResults;
            _allocator = match._searcher.Allocator;
            _nullIsSmallest = match.GetNullIsSmallestForClause(_comparerId);
            if (match._searcher.TryGetPostingListForNull(fieldName, out _, out _nullTermContainerId) == false)
                _nullTermContainerId = SortingHelpers.InvalidTermId;
            if (match._searcher.TryGetPostingListForNonExisting(fieldName, out _, out _nonExistingTermContainerId) == false)
                _nonExistingTermContainerId = SortingHelpers.InvalidTermId;
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

            _lookup.GetFor(batchResults, batchTermIds, SortingHelpers.MissingTermId);
            SortingHelpers.ReplaceNullAndNonExistingTermIds(batchTermIds, _nonExistingTermContainerId, _nullTermContainerId, SortingHelpers.MissingTermId);
            Container.GetAll(llt, batchTermIds, new Span<UnmanagedSpan>(batchTerms, batchTermIds.Length), SortingHelpers.MissingTermId, pageLocator);
            var documents = MemoryMarshal.Cast<long, int>(batchTermIds)[..(batchTermIds.Length)];
            for (int i = 0; i < batchTermIds.Length; i++)
                documents[i] = i;

            var heapCapacity = match._take == -1 ? batchResults.Length : Math.Min(match._take, batchResults.Length);
            using var _ = _allocator.Allocate(heapCapacity, out Span<ByteString> terms);
            var secondaryComparers = new IndirectComparer2<TComparer2, TComparer3>(ref match, comparer2, comparer3);
            var heapSorter = HeapSorterBuilder.BuildCompoundAlphanumericalSorter(documents.Slice(0, heapCapacity), terms, _allocator, orderMetadata[0].Ascending == false, secondaryComparers, match.GetNullIsSmallestForClause(_comparerId));

            ContextBoundNativeList<int> nullIndexes = default;
            for (int i = 0; i < batchTermIds.Length; i++)
            {
                var term = batchTerms[i];
                if (term.Address == null)
                {
                    if (nullIndexes.HasContext == false)
                        nullIndexes = new ContextBoundNativeList<int>(_allocator);
                    nullIndexes.Add(i);
                    continue;
                }
                heapSorter.Insert(i, _reader.GetDecodedTerm(_dictionaryId, batchTerms[i]));
            }

            heapSorter.Fill(batchResults, ref match._results, ref match._scoresResults, match._secondaryScoreBuffer, nullIndexes);
            nullIndexes.Dispose();
        }

        public int Compare(UnmanagedSpan x, UnmanagedSpan y)
        {
            throw new NotSupportedException($"Method `{nameof(Compare)} for `{nameof(UnmanagedSpan)}` should never be used.");
        }

        public int Compare(int x, int y)
        {
            if (_lookup == null)
                return 0;

            Span<long> buffer = [_batchResults[x], _batchResults[y], -1, -1];
            var swap = buffer[0] > buffer[1];
            if (swap) 
                buffer[..2].Reverse();
            _lookup.GetFor(buffer[..2], buffer[2..], long.MinValue);
            
            if (swap) 
                buffer[2..].Reverse();
            
            if (buffer[2] == _nullTermContainerId || buffer[2] == _nonExistingTermContainerId)
                buffer[2] = long.MinValue;
            if (buffer[3] == _nullTermContainerId || buffer[3] == _nonExistingTermContainerId)
                buffer[3] = long.MinValue;

            var xIsNull = buffer[2] == long.MinValue;
            var yIsNull = buffer[3] == long.MinValue;

            if (xIsNull || yIsNull)
            {
                if (xIsNull && yIsNull) return 0;
                return _nullIsSmallest 
                    ? (xIsNull ? -1 : 1) 
                    : (xIsNull ? 1 : -1);
            }

            _reader.GetDecodedTermsByIds(_dictionaryId, _batchResults[x], out var xTerm, _batchResults[y], out var yTerm);
            return AlphanumericalComparer.Instance.Compare(xTerm, yTerm);
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
        private double _missingValueForInnerSorter;
        
        public Slice GetSortFieldName(ref SortingMultiMatch<TInner> match) => match._orderMetadata[_comparerId].Field.FieldName;

        public void Init(ref SortingMultiMatch<TInner> match, UnmanagedSpan<long> batchResults, int comparerId)
        {
            _batchResults = batchResults;
            _comparerId = comparerId;
            _center = (match._orderMetadata[_comparerId].Point.X, match._orderMetadata[_comparerId].Point.Y);
            _units = match._orderMetadata[_comparerId].Units;
            _round = match._orderMetadata[_comparerId].Round;
            _reader = match._searcher.SpatialReader(match._orderMetadata[_comparerId].Field.FieldName);

            if (comparerId > 0)
            {
                _missingValueForInnerSorter = match.GetNullIsSmallestForClause(_comparerId) ? double.MinValue : double.MaxValue;
            }
        }

        public void SortBatch<TComparer2, TComparer3>(ref SortingMultiMatch<TInner> match, LowLevelTransaction llt, PageLocator pageLocator,
            UnmanagedSpan<long> batchResults, Span<long> batchTermIds,
            UnmanagedSpan* batchTerms, OrderMetadata[] orderMetadata, TComparer2 comparer2, TComparer3 comparer3)
            where TComparer2 : struct, IComparer<UnmanagedSpan>, IComparer<int>, IEntryComparer
            where TComparer3 : struct, IComparer<UnmanagedSpan>, IComparer<int>, IEntryComparer
        {
            if (_reader.IsValid == false) // field does not exist, so arbitrary sort order, whatever query said goes
            {
                match._results.AddRange(batchResults);
                return;
            }

            var descending = orderMetadata[0].Ascending == false;
            var heapSize = Math.Min(match._take, batchResults.Length);
            heapSize = heapSize < 0 ? batchResults.Length : heapSize;
            
            var indexes = MemoryMarshal.Cast<long, int>(batchTermIds)[..(batchTermIds.Length)];
            using var _ = llt.Allocator.Allocate(heapSize, out Span<SpatialResult> terms);


            var heapSorter = HeapSorterBuilder.BuildSingleNumericalSorter<SpatialResult>(indexes.Slice(0, heapSize), terms, descending, match.GetNullIsSmallestForClause(_comparerId));
            


            for (int i = 0; i < batchResults.Length; i++)
            {
                SpatialResult distance; 
                if (_reader.TryGetSpatialPoint(batchResults[i], out var coords) == false)
                {
                    var distanceForMissing = match.GetNullIsSmallestForClause(_comparerId) ? double.MinValue : double.MaxValue;
                    distance = new SpatialResult() { Distance = distanceForMissing, Latitude = Double.NaN, Longitude = Double.NaN };
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
                heapSorter.FillWithTerms(batchResults, ref match._results, ref match._distancesResults, ref match._scoresResults, match._secondaryScoreBuffer);
            else
                heapSorter.Fill(batchResults, ref match._results, ref match._scoresResults, match._secondaryScoreBuffer);
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
                ? _missingValueForInnerSorter 
                : SpatialUtils.GetGeoDistance(coords, _center, _round, _units);

            double yDistance = _reader.TryGetSpatialPoint(_batchResults[y], out coords) == false 
                ? _missingValueForInnerSorter
                : SpatialUtils.GetGeoDistance(coords, _center, _round, _units);

            return xDistance.CompareTo(yDistance);
        }
    }
}
