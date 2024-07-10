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

    private struct EntryComparerByTerm : IEntryComparer, IComparer<UnmanagedSpan>, IComparer<int>
    {
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

            var heapSize = Math.Min(match._take, batchResults.Length);
            heapSize = heapSize < 0 ? batchResults.Length : heapSize;
            var indexes = MemoryMarshal.Cast<long, int>(batchTermIds)[..(batchTermIds.Length)];
            var secondaryComparer = new SortingMultiMatch<TInner>.IndirectComparer2<TComparer2, TComparer3>(ref match, comparer2, comparer3);
            using var _ = llt.Allocator.Allocate(heapSize, out Span<UnmanagedSpan> terms);
            var sorter = HeapSorterBuilder.BuildCompoundCompactKeySorter(indexes.Slice(0, heapSize), terms, orderMetadata[0].Ascending == false, secondaryComparer);
           
            for (int i = 0; i < indexes.Length; i++)
                sorter.Insert(i, batchTerms[i]);
            
            sorter.Fill(batchResults, ref match._results);
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
            return CompactKeyComparer.Compare(x, y);
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
            // load terms for documents
            _lookup.GetFor(batchResults, batchTermIds, BitConverter.DoubleToInt64Bits(double.MinValue));

            var heapSize = Math.Min(match._take, batchResults.Length);
            heapSize = heapSize < 0 ? batchResults.Length : heapSize;
            
            using var _ = llt.Allocator.Allocate(heapSize, out Span<long> terms);
            var indexes = MemoryMarshal.Cast<long, int>(batchTermIds)[..(batchTermIds.Length)];
            var secondaryComparer = new IndirectComparer2<TComparer2, TComparer3>(ref match, comparer2, comparer3);
            var heapSorter = HeapSorterBuilder.BuildCompoundNumericalSorter(indexes.Slice(0, heapSize), terms, orderMetadata[0].Ascending == false, secondaryComparer);
                
            for (int i = 0; i < indexes.Length; i++)
                heapSorter.Insert(i, batchTermIds[i]);

            heapSorter.Fill(batchResults, ref match._results);
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
            // load terms for documents
            _lookup.GetFor(batchResults, batchTermIds, BitConverter.DoubleToInt64Bits(double.MinValue));

            var heapSize = Math.Min(match._take, batchResults.Length);
            heapSize = heapSize < 0 ? batchResults.Length : heapSize;
            
            using var _ = llt.Allocator.Allocate(heapSize, out Span<double> terms);
            var indexes = MemoryMarshal.Cast<long, int>(batchTermIds)[..(batchTermIds.Length)];
            var secondaryComparer = new IndirectComparer2<TComparer2, TComparer3>(ref match, comparer2, comparer3);
            var heapSorter = HeapSorterBuilder.BuildCompoundNumericalSorter(indexes.Slice(0, heapSize), terms, orderMetadata[0].Ascending == false, secondaryComparer);
                
            for (int i = 0; i < indexes.Length; i++)
                heapSorter.Insert(i, BitConverter.Int64BitsToDouble(batchTermIds[i]));

            heapSorter.Fill(batchResults, ref match._results);
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
        public Slice GetSortFieldName(ref SortingMultiMatch<TInner> match) => match._orderMetadata[_comparerId].Field.FieldName;

        public void Init(ref SortingMultiMatch<TInner> match, UnmanagedSpan<long> batchResults, int comparerId)
        {
            _comparerId = comparerId;
            _reader = match._searcher.TermsReaderFor(match._orderMetadata[_comparerId].Field.FieldName);
            _dictionaryId = match._searcher.GetDictionaryIdFor(match._orderMetadata[_comparerId].Field.FieldName);
            _lookup = match._searcher.EntriesToTermsReader(match._orderMetadata[_comparerId].Field.FieldName);
            _batchResults = batchResults;
            _allocator = match._searcher.Allocator;
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
            var documents = MemoryMarshal.Cast<long, int>(batchTermIds)[..(batchTermIds.Length)];
            for (int i = 0; i < batchTermIds.Length; i++)
                documents[i] = i;

            var heapCapacity = match._take == -1 ? batchResults.Length : Math.Min(match._take, batchResults.Length);
            using var _ = _allocator.Allocate(heapCapacity, out Span<ByteString> terms);
            var secondaryComparers = new IndirectComparer2<TComparer2, TComparer3>(ref match, comparer2, comparer3);
            var heapSorter = HeapSorterBuilder.BuildCompoundAlphanumericalSorter(documents.Slice(0, heapCapacity), terms, _allocator, orderMetadata[0].Ascending == false, secondaryComparers);
           
            for (int i = 0; i < batchTermIds.Length; i++)
                heapSorter.Insert(i, _reader.GetDecodedTerm(_dictionaryId, batchTerms[i]));

            heapSorter.Fill(batchResults, ref match._results);
        }
        
        public int Compare(UnmanagedSpan x, UnmanagedSpan y)
        {
            throw new NotSupportedException($"Method `{nameof(Compare)} for `{nameof(UnmanagedSpan)}` should never be used.");
        }

        public int Compare(int x, int y)
        {
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
}
