using System;
using System.Buffers.Binary;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Reflection.Metadata;
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

namespace Corax.Queries.SortingMatches;

public unsafe partial struct NewMultiSortingMatch<TInner> : IQueryMatch
    where TInner : IQueryMatch
{
     private interface IEntryComparer
     {
         Slice GetSortFieldName(ref NewMultiSortingMatch<TInner> match);
         void Init(ref NewMultiSortingMatch<TInner> match, UnmanagedSpan<long> batchResults, int comparerId);

         void SortBatch<TComparer2, TComparer3, TComparer4, TComparer5, TComparer6, TComparer7, TComparer8>(ref NewMultiSortingMatch<TInner> match, LowLevelTransaction llt, PageLocator pageLocator, UnmanagedSpan<long> batchResults, Span<long> batchTermIds, UnmanagedSpan* batchTerms, OrderMetadata[] orderMetadata, TComparer2 comparer2, TComparer3 comparer3, TComparer4 comparer4, TComparer5 comparer5, TComparer6 comparer6, TComparer7 comparer7, TComparer8 comparer8)            
             where TComparer2 : struct, IComparer<UnmanagedSpan>, IComparer<int>, IEntryComparer
             where TComparer3 : struct, IComparer<UnmanagedSpan>, IComparer<int>, IEntryComparer
             where TComparer4 : struct, IComparer<UnmanagedSpan>, IComparer<int>, IEntryComparer
             where TComparer5 : struct, IComparer<UnmanagedSpan>, IComparer<int>, IEntryComparer
             where TComparer6 : struct, IComparer<UnmanagedSpan>, IComparer<int>, IEntryComparer
             where TComparer7 : struct, IComparer<UnmanagedSpan>, IComparer<int>, IEntryComparer
             where TComparer8 : struct, IComparer<UnmanagedSpan>, IComparer<int>, IEntryComparer;
     }
     
     
    private struct Descending<TInnerCmp> : IEntryComparer, IComparer<UnmanagedSpan>, IComparer<int> 
        where TInnerCmp : struct,  IEntryComparer, IComparer<UnmanagedSpan>, IComparer<int>
    {
        private TInnerCmp cmp;

        public Descending(TInnerCmp cmp)
        {
            this.cmp = cmp;
        }

        public Descending()
        {
            cmp = new();
        }

        public Slice GetSortFieldName(ref NewMultiSortingMatch<TInner> match)
        {
            return cmp.GetSortFieldName(ref match);
        }
        
        public void Init(ref NewMultiSortingMatch<TInner> match, UnmanagedSpan<long> batchResults, int comparerId)
        {
            cmp.Init(ref match, batchResults, comparerId);
        }
        
        public void SortBatch<TComparer2, TComparer3, TComparer4, TComparer5, TComparer6, TComparer7, TComparer8>(ref NewMultiSortingMatch<TInner> match, LowLevelTransaction llt, PageLocator pageLocator, UnmanagedSpan<long> batchResults, Span<long> batchTermIds, UnmanagedSpan* batchTerms, OrderMetadata[] orderMetadata, TComparer2 comparer2,
             TComparer3 comparer3, TComparer4 comparer4, TComparer5 comparer5, TComparer6 comparer6, TComparer7 comparer7, TComparer8 comparer8)
            where TComparer2 : struct, IComparer<UnmanagedSpan>, IComparer<int>, IEntryComparer
            where TComparer3 : struct, IComparer<UnmanagedSpan>, IComparer<int>, IEntryComparer
            where TComparer4 : struct, IComparer<UnmanagedSpan>, IComparer<int>, IEntryComparer
            where TComparer5 : struct, IComparer<UnmanagedSpan>, IComparer<int>, IEntryComparer
            where TComparer6 : struct, IComparer<UnmanagedSpan>, IComparer<int>, IEntryComparer
            where TComparer7 : struct, IComparer<UnmanagedSpan>, IComparer<int>, IEntryComparer
            where TComparer8 : struct, IComparer<UnmanagedSpan>, IComparer<int>, IEntryComparer
        {
            cmp.SortBatch<TComparer2, TComparer3, TComparer4, TComparer5, TComparer6, TComparer7, TComparer8>(ref match, llt,
                pageLocator, batchResults, batchTermIds, batchTerms, orderMetadata, comparer2, comparer3, comparer4, comparer5, comparer6, comparer7, comparer8);
        }
        
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Compare(UnmanagedSpan x, UnmanagedSpan y)
        {
            return cmp.Compare(y, x); // note the revered args
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Compare(int x, int y) => cmp.Compare(y, x); // note the reversed args
    }

    private struct EntryComparerByScore : IEntryComparer, IComparer<UnmanagedSpan>, IComparer<int>
    {
        public Slice GetSortFieldName(ref NewMultiSortingMatch<TInner> match)
        {
            throw new NotImplementedException("Scoring has no field name");
        }

        public void Init(ref NewMultiSortingMatch<TInner> match)
        {
        }

        public void Init(ref NewMultiSortingMatch<TInner> match, UnmanagedSpan<long> batchResults, int comparerId)
        {
            throw new NotImplementedException();
        }

        public void SortBatch<TComparer2, TComparer3, TComparer4, TComparer5, TComparer6, TComparer7, TComparer8>(ref NewMultiSortingMatch<TInner> match, LowLevelTransaction llt, PageLocator pageLocator, UnmanagedSpan<long> batchResults, Span<long> batchTermIds, UnmanagedSpan* batchTerms, OrderMetadata[] orderMetadata, TComparer2 comparer2, TComparer3 comparer3, TComparer4 comparer4, TComparer5 comparer5, TComparer6 comparer6, TComparer7 comparer7, TComparer8 comparer8)
            where TComparer2 : struct, IComparer<UnmanagedSpan>, IComparer<int>, IEntryComparer
            where TComparer3 : struct, IComparer<UnmanagedSpan>, IComparer<int>, IEntryComparer
            where TComparer4 : struct, IComparer<UnmanagedSpan>, IComparer<int>, IEntryComparer
            where TComparer5 : struct, IComparer<UnmanagedSpan>, IComparer<int>, IEntryComparer
            where TComparer6 : struct, IComparer<UnmanagedSpan>, IComparer<int>, IEntryComparer
            where TComparer7 : struct, IComparer<UnmanagedSpan>, IComparer<int>, IEntryComparer
            where TComparer8 : struct, IComparer<UnmanagedSpan>, IComparer<int>, IEntryComparer
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
            
            EntryComparerHelper.IndirectSort<EntryComparerByScore, TComparer2, TComparer3, TComparer4, TComparer5, TComparer6, TComparer7, TComparer8>(indexes, batchTerms, new(), comparer2, comparer3, comparer4, comparer5, comparer6, comparer7, comparer8, orderMetadata);

            for (int i = 0; i < indexes.Length; i++)
            {
                match._results.Add(batchResults[indexes[i]]);
            }
        }

        private static void BoostDocuments(NewMultiSortingMatch<TInner> match, Span<long> batchResults, Span<float> readScores)
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

        public int Compare(int x, int y)
        {
            throw new NotImplementedException();
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
            var match = AdvMemory.Compare(xItem.Address + 1, yItem.Address + 1, Math.Min(xItem.Length - 1, yItem.Length - 1));
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
        private Lookup<long> _lookup;
        private UnmanagedSpan<long> _batchResults;
        private int _comparerId;
        
        public Slice GetSortFieldName(ref NewMultiSortingMatch<TInner> match) => match._orderMetadata[_comparerId].Field.FieldName;
        
        public void Init(ref NewMultiSortingMatch<TInner> match, UnmanagedSpan<long> batchResults, int comparerId)
        {
            _comparerId = comparerId;
            _lookup = match._searcher.EntriesToTermsReader(match._orderMetadata[_comparerId].Field.FieldName);
            _batchResults = batchResults;
        }

        public void SortBatch<TComparer2, TComparer3, TComparer4, TComparer5, TComparer6, TComparer7, TComparer8>(ref NewMultiSortingMatch<TInner> match, LowLevelTransaction llt, PageLocator pageLocator, UnmanagedSpan<long> batchResults, Span<long> batchTermIds, UnmanagedSpan* batchTerms, OrderMetadata[] orderMetadata, TComparer2 comparer2,
            TComparer3 comparer3, TComparer4 comparer4, TComparer5 comparer5, TComparer6 comparer6, TComparer7 comparer7, TComparer8 comparer8) 
            where TComparer2 : struct, IComparer<UnmanagedSpan>, IComparer<int>, IEntryComparer 
            where TComparer3 : struct, IComparer<UnmanagedSpan>, IComparer<int>, IEntryComparer 
            where TComparer4 : struct, IComparer<UnmanagedSpan>, IComparer<int>, IEntryComparer 
            where TComparer5 : struct, IComparer<UnmanagedSpan>, IComparer<int>, IEntryComparer
            where TComparer6 : struct, IComparer<UnmanagedSpan>, IComparer<int>, IEntryComparer
            where TComparer7 : struct, IComparer<UnmanagedSpan>, IComparer<int>, IEntryComparer 
            where TComparer8 : struct, IComparer<UnmanagedSpan>, IComparer<int>, IEntryComparer
        {
            if (_lookup == null) // field does not exist, so arbitrary sort order, whatever query said goes
            {
                match._results.Add(batchResults);
                return;
            }
            _lookup.GetFor(batchResults, batchTermIds, long.MinValue);
            Container.GetAll(llt, batchTermIds, batchTerms, long.MinValue, pageLocator);
            var indirectComparer = new IndirectComparer<CompactKeyComparer, TComparer2, TComparer3, TComparer4, TComparer5, TComparer6, TComparer7, TComparer8>(batchTerms, new CompactKeyComparer(), comparer2, comparer3, comparer4, comparer5, comparer6, comparer7, comparer8);
            var indexes = SortByTerms(ref match, batchTermIds, batchTerms, orderMetadata[0].Ascending == false, indirectComparer);
            for (int i = 0; i < indexes.Length; i++)
            {
                int bIdx = indexes[i];
                match._results.Add(batchResults[bIdx]);
            }
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

        private static Span<int> SortByTerms<TComparer>(ref NewMultiSortingMatch<TInner> match, Span<long> buffer, UnmanagedSpan* batchTerms, bool isDescending,
            TComparer tieBreaker)
            where TComparer : struct, IComparer<long>
        {
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

        public int Compare(int x, int y)
        {
            throw new NotImplementedException();
        }
    }
    
    private struct EntryComparerByLong : IEntryComparer, IComparer<UnmanagedSpan>, IComparer<int>
    {
        private Lookup<long> _lookup;
        private UnmanagedSpan<long> _batchResults;
        private int _comparerId;
        
        public Slice GetSortFieldName(ref NewMultiSortingMatch<TInner> match)
        {
            IndexFieldsMappingBuilder.GetFieldNameForLongs(match._searcher.Allocator, match._orderMetadata[_comparerId].Field.FieldName, out var lngName);
            return lngName;
        }

        public void Init(ref NewMultiSortingMatch<TInner> match, UnmanagedSpan<long> batchResults, int comparerId)
        {
            _comparerId = comparerId;
            _lookup = match._searcher.EntriesToTermsReader(GetSortFieldName(ref match));
            _batchResults = batchResults;
        }

        public void SortBatch<TComparer2, TComparer3, TComparer4, TComparer5, TComparer6, TComparer7, TComparer8>(ref NewMultiSortingMatch<TInner> match, LowLevelTransaction llt, PageLocator pageLocator, UnmanagedSpan<long> batchResults, Span<long> batchTermIds, UnmanagedSpan* batchTerms, OrderMetadata[] orderMetadata, TComparer2 comparer2,
            TComparer3 comparer3, TComparer4 comparer4, TComparer5 comparer5, TComparer6 comparer6, TComparer7 comparer7, TComparer8 comparer8) 
            where TComparer2 : struct, IComparer<UnmanagedSpan>, IComparer<int>, IEntryComparer 
            where TComparer3 : struct, IComparer<UnmanagedSpan>, IComparer<int>, IEntryComparer 
            where TComparer4 : struct, IComparer<UnmanagedSpan>, IComparer<int>, IEntryComparer 
            where TComparer5 : struct, IComparer<UnmanagedSpan>, IComparer<int>, IEntryComparer 
            where TComparer6 : struct, IComparer<UnmanagedSpan>, IComparer<int>, IEntryComparer 
            where TComparer7 : struct, IComparer<UnmanagedSpan>, IComparer<int>, IEntryComparer 
            where TComparer8 : struct, IComparer<UnmanagedSpan>, IComparer<int>, IEntryComparer
        {
            if (_lookup == null) // field does not exist, so arbitrary sort order, whatever query said goes
            {
                match._results.Add(batchResults);
                return;
            }
            _lookup.GetFor(batchResults, batchTermIds, long.MinValue);
            var indexes = EntryComparerHelper.NumericSortBatch(batchTermIds, batchTerms, new EntryComparerByLong(), comparer2, comparer3, comparer4, comparer5, comparer6, comparer7, comparer8, orderMetadata);
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

            Span<long> buffer = stackalloc long[4]
            {
                _batchResults[x],
                _batchResults[y],
                -1, -1
            };
            _lookup.GetFor(buffer.Slice(0,2), buffer.Slice(2), long.MinValue);

            return buffer[2].CompareTo(buffer[3]);
        }
    }

    private struct EmptyComparer : IEntryComparer, IComparer<UnmanagedSpan>, IComparer<int>
    {
        public Slice GetSortFieldName(ref NewMultiSortingMatch<TInner> match)
        {
            throw new NotSupportedException($"{nameof(EmptyComparer)} is for type-relaxation. You should not use it");
        }
        
        public void Init(ref NewMultiSortingMatch<TInner> match, UnmanagedSpan<long> batchResults, int comparerId)
        {
        }

        public void SortBatch<TComparer2, TComparer3, TComparer4, TComparer5, TComparer6, TComparer7, TComparer8>(ref NewMultiSortingMatch<TInner> match, LowLevelTransaction llt,
            PageLocator pageLocator, UnmanagedSpan<long> batchResults, Span<long> batchTermIds, UnmanagedSpan* batchTerms, OrderMetadata[] orderMetadata, TComparer2 comparer2,
            TComparer3 comparer3, TComparer4 comparer4, TComparer5 comparer5, TComparer6 comparer6, TComparer7 comparer7, TComparer8 comparer8) where TComparer2 : struct, IComparer<UnmanagedSpan>, IComparer<int>, IEntryComparer where TComparer3 : struct, IComparer<UnmanagedSpan>, IComparer<int>, IEntryComparer where TComparer4 : struct, IComparer<UnmanagedSpan>, IComparer<int>, IEntryComparer where TComparer5 : struct, IComparer<UnmanagedSpan>, IComparer<int>, IEntryComparer where TComparer6 : struct, IComparer<UnmanagedSpan>, IComparer<int>, IEntryComparer where TComparer7 : struct, IComparer<UnmanagedSpan>, IComparer<int>, IEntryComparer where TComparer8 : struct, IComparer<UnmanagedSpan>, IComparer<int>, IEntryComparer
        {
            throw new NotSupportedException($"{nameof(EmptyComparer)} is for type-relaxation. You should not use it");
        }
        
        public int Compare(UnmanagedSpan x, UnmanagedSpan y)
        {
            return 0; //how it goes
        }

        public int Compare(int x, int y) => 0; // how it goes
    }
    
    private struct EntryComparerByDouble : IEntryComparer, IComparer<UnmanagedSpan>, IComparer<int>
    {
        private int _comparerId;
        private Lookup<long> _lookup;
        private UnmanagedSpan<long> _batchResults;
        
        public void SortBatch<TComparer2, TComparer3, TComparer4, TComparer5, TComparer6, TComparer7, TComparer8>(ref NewMultiSortingMatch<TInner> match, LowLevelTransaction llt, PageLocator pageLocator, UnmanagedSpan<long> batchResults, Span<long> batchTermIds, UnmanagedSpan* batchTerms, OrderMetadata[] orderMetadata, TComparer2 comparer2,
            TComparer3 comparer3, TComparer4 comparer4, TComparer5 comparer5, TComparer6 comparer6, TComparer7 comparer7, TComparer8 comparer8) 
            where TComparer2 : struct, IComparer<UnmanagedSpan>, IComparer<int>, IEntryComparer 
            where TComparer3 : struct, IComparer<UnmanagedSpan>, IComparer<int>, IEntryComparer 
            where TComparer4 : struct, IComparer<UnmanagedSpan>, IComparer<int>, IEntryComparer 
            where TComparer5 : struct, IComparer<UnmanagedSpan>, IComparer<int>, IEntryComparer 
            where TComparer6 : struct, IComparer<UnmanagedSpan>, IComparer<int>, IEntryComparer 
            where TComparer7 : struct, IComparer<UnmanagedSpan>, IComparer<int>, IEntryComparer 
            where TComparer8 : struct, IComparer<UnmanagedSpan>, IComparer<int>, IEntryComparer
        {
            if (_lookup == null) // field does not exist, so arbitrary sort order, whatever query said goes
            {
                match._results.Add(batchResults);
                return;
            }
            
            _lookup.GetFor(batchResults, batchTermIds, BitConverter.DoubleToInt64Bits(double.MinValue));
            var indexes = EntryComparerHelper.NumericSortBatch(batchTermIds, batchTerms, new EntryComparerByDouble(), comparer2, comparer3, comparer4, comparer5, comparer6, comparer7, comparer8, orderMetadata);
            for (int i = 0; i < indexes.Length; i++)
            {
                match._results.Add(batchResults[indexes[i]]);
            }
        }

        public Slice GetSortFieldName(ref NewMultiSortingMatch<TInner> match)
        {
            IndexFieldsMappingBuilder.GetFieldNameForDoubles(match._searcher.Allocator, match._orderMetadata[_comparerId].Field.FieldName, out var dblName);
            return dblName;
        }

        public void Init(ref NewMultiSortingMatch<TInner> match, UnmanagedSpan<long> batchResults, int comparerId)
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

            Span<long> buffer = stackalloc long[4]
            {
                _batchResults[x], 
                _batchResults[y], 
                -1, 
                -1
            };
            _lookup.GetFor(buffer.Slice(0,2), buffer.Slice(2), BitConverter.DoubleToInt64Bits(double.MinValue));

            return BitConverter.Int64BitsToDouble(buffer[2]).CompareTo(BitConverter.Int64BitsToDouble(buffer[3]));
        }
        
    }

    private struct EntryComparerByTermAlphaNumeric : IEntryComparer, IComparer<UnmanagedSpan>, IComparer<int>
    {
        private TermsReader _reader;
        private long _dictionaryId;
        private Lookup<long> _lookup;
        private UnmanagedSpan<long> _batchResults;
        private int _comparerId;
        
        public Slice GetSortFieldName(ref NewMultiSortingMatch<TInner> match) => match._orderMetadata[_comparerId].Field.FieldName;
        
        public void Init(ref NewMultiSortingMatch<TInner> match, UnmanagedSpan<long> batchResults, int comparerId)
        {
            _comparerId = comparerId;
            _reader = match._searcher.TermsReaderFor(match._orderMetadata[_comparerId].Field.FieldName);
            _dictionaryId = match._searcher.GetDictionaryIdFor(match._orderMetadata[_comparerId].Field.FieldName);
            _lookup = match._searcher.EntriesToTermsReader(match._orderMetadata[_comparerId].Field.FieldName);
            _batchResults = batchResults;
        }

        public void SortBatch<TComparer2, TComparer3, TComparer4, TComparer5, TComparer6, TComparer7, TComparer8>(ref NewMultiSortingMatch<TInner> match, LowLevelTransaction llt, PageLocator pageLocator, UnmanagedSpan<long> batchResults, Span<long> batchTermIds, UnmanagedSpan* batchTerms, OrderMetadata[] orderMetadata, TComparer2 comparer2, TComparer3 comparer3, TComparer4 comparer4, TComparer5 comparer5, TComparer6 comparer6, TComparer7 comparer7, TComparer8 comparer8) 
            where TComparer2 : struct, IComparer<UnmanagedSpan>, IComparer<int>, IEntryComparer 
            where TComparer3 : struct, IComparer<UnmanagedSpan>, IComparer<int>, IEntryComparer 
            where TComparer4 : struct, IComparer<UnmanagedSpan>, IComparer<int>, IEntryComparer 
            where TComparer5 : struct, IComparer<UnmanagedSpan>, IComparer<int>, IEntryComparer 
            where TComparer6 : struct, IComparer<UnmanagedSpan>, IComparer<int>, IEntryComparer 
            where TComparer7 : struct, IComparer<UnmanagedSpan>, IComparer<int>, IEntryComparer 
            where TComparer8 : struct, IComparer<UnmanagedSpan>, IComparer<int>, IEntryComparer
        {
            if (_lookup == null) // field does not exist, so arbitrary sort order, whatever query said goes
            {
                match._results.Add(batchResults);
                return;
            }
            _lookup.GetFor(batchResults, batchTermIds, long.MinValue);
            Container.GetAll(llt, batchTermIds, batchTerms, long.MinValue, pageLocator);
            var indexes = MemoryMarshal.Cast<long, int>(batchTermIds)[..(batchTermIds.Length)];
            for (int i = 0; i < batchTermIds.Length; i++)
            {
                indexes[i] = i;
            }
            EntryComparerHelper.IndirectSort(indexes, batchTerms, this, comparer2, comparer3, comparer4, comparer5, comparer6, comparer7, comparer8, orderMetadata);
            for (int i = 0; i < indexes.Length; i++)
            {
                match._results.Add(batchResults[indexes[i]]);
            }
        }


        public int Compare(UnmanagedSpan x, UnmanagedSpan y)
        {
            _reader.GetDecodedTerms(_dictionaryId, x, out var xTerm, y, out var yTerm);
            return -1; //todo
            // return Comparers.LegacyNewMultiSortingMatch.BasicComparers.CompareAlphanumericAscending(xTerm, yTerm);
        }

        public int Compare(int x, int y)
        {
            return String.Compare(_reader.GetTermFor(_batchResults[x]), _reader.GetTermFor(_batchResults[y]), StringComparison.Ordinal);
        }
    }
        
    // private struct EntryComparerBySpatial : IEntryComparer, IComparer<UnmanagedSpan>
    // {
    //     private SpatialReader _reader;
    //     private (double X, double Y) _center;
    //     private SpatialUnits _units;
    //     private double _round;
    //
    //     public Slice GetSortFieldName(ref NewMultiSortingMatch<TInner> match) => match._orderMetadata.Field.FieldName;
    //
    //     public void Init(ref NewMultiSortingMatch<TInner> match)
    //     {
    //         _center = (match._orderMetadata.Point.X, match._orderMetadata.Point.Y);
    //         _units = match._orderMetadata.Units;
    //         _round = match._orderMetadata.Round;
    //         _reader = match._searcher.SpatialReader(match._orderMetadata.Field.FieldName);
    //     }
    //
    //     public void SortBatch(ref NewMultiSortingMatch<TInner> match, LowLevelTransaction llt, PageLocator pageLocator, Span<long> batchResults, Span<long> batchTermIds,
    //         UnmanagedSpan* batchTerms,
    //         bool descending = false)
    //     {
    //         if (_reader.IsValid == false) // field does not exist, so arbitrary sort order, whatever query said goes
    //         {
    //             match._results.Add(batchResults);
    //             return;
    //         }
    //         var indexes = MemoryMarshal.Cast<long, int>(batchTermIds)[..(batchTermIds.Length)];
    //         for (int i = 0; i < batchResults.Length; i++)
    //         {
    //             double distance;
    //             if (_reader.TryGetSpatialPoint(batchResults[i], out var coords) == false)
    //             {
    //                 // always at the bottom, then, desc & asc
    //                 distance = descending ? double.MinValue : double.MaxValue;
    //             }
    //             else
    //             {
    //                 distance = SpatialUtils.GetGeoDistance(coords, _center, _round, _units);
    //             }
    //
    //             batchTerms[i] = new UnmanagedSpan(distance);
    //             indexes[i] = i;
    //         }
    //
    //         EntryComparerHelper.IndirectSort<EntryComparerByDouble>(indexes, batchTerms, descending);
    //         for (int i = 0; i < indexes.Length; i++)
    //         {
    //             match._results.Add(batchResults[indexes[i]]);
    //         }
    //     }
    //
    //     public int Compare(UnmanagedSpan x, UnmanagedSpan y)
    //     {
    //         return x.Double.CompareTo(y.Double);
    //     }
    // }


    private readonly struct IndirectComparer<TComparer1, TComparer2, TComparer3, TComparer4, TComparer5, TComparer6,
        TComparer7, TComparer8> : IComparer<long>, IComparer<int>
        where TComparer1 : struct, IComparer<UnmanagedSpan>
        where TComparer2 : struct, IComparer<int>
        where TComparer3 : struct, IComparer<int>
        where TComparer4 : struct, IComparer<int>
        where TComparer5 : struct, IComparer<int>
        where TComparer6 : struct, IComparer<int>
        where TComparer7 : struct, IComparer<int>
        where TComparer8 : struct, IComparer<int>
    {
        private readonly UnmanagedSpan* _terms;
        private readonly TComparer1 _cmp1;
        private readonly TComparer2 _cmp2;
        private readonly TComparer3 _cmp3;
        private readonly TComparer4 _cmp4;
        private readonly TComparer5 _cmp5;
        private readonly TComparer6 _cmp6;
        private readonly TComparer7 _cmp7;
        private readonly TComparer8 _cmp8;

        private readonly int _maxDegreeOfInnerComparer;
        
        public IndirectComparer(UnmanagedSpan* terms, TComparer1 entryComparer, TComparer2 cmp2, TComparer3 cmp3, TComparer4 cmp4, TComparer5 cmp5, TComparer6 cmp6, TComparer7 cmp7, TComparer8 cmp8)
        {
            _terms = terms;
            _cmp1 = entryComparer;
            _cmp2 = cmp2;
            _cmp3 = cmp3;
            _cmp4 = cmp4;
            _cmp5 = cmp5;
            _cmp6 = cmp6;
            _cmp7 = cmp7;
            _cmp8 = cmp8;

            if (typeof(TComparer1) == typeof(EmptyComparer))
                _maxDegreeOfInnerComparer = 0;
            else if (typeof(TComparer2) == typeof(EmptyComparer))
                _maxDegreeOfInnerComparer = 1;
            else if (typeof(TComparer3) == typeof(EmptyComparer))
                _maxDegreeOfInnerComparer = 2;
            else if (typeof(TComparer4) == typeof(EmptyComparer))
                _maxDegreeOfInnerComparer = 3;
            else if (typeof(TComparer5) == typeof(EmptyComparer))
                _maxDegreeOfInnerComparer = 4;
            else if (typeof(TComparer6) == typeof(EmptyComparer))
                _maxDegreeOfInnerComparer = 5;
            else if (typeof(TComparer7) == typeof(EmptyComparer))
                _maxDegreeOfInnerComparer = 6;
            else if (typeof(TComparer8) == typeof(EmptyComparer))
                _maxDegreeOfInnerComparer = 7;

        }

        public int Compare(long x, long y)
        {
            var xIdx = (ushort)x & 0X7FFF;
            var yIdx = (ushort)y & 0X7FFF;
            Debug.Assert(yIdx < SortBatchSize && xIdx < SortBatchSize);
            
            var cmp = 0;
            for (int comparerId = 0; cmp == 0 && comparerId < _maxDegreeOfInnerComparer; ++comparerId)
            {
                cmp = comparerId switch
                {
                    0 => _cmp1.Compare(_terms[xIdx], _terms[yIdx]),
                    1 => _cmp2.Compare(xIdx, yIdx),
                    2 => _cmp3.Compare(xIdx, yIdx),
                    3 => _cmp4.Compare(xIdx, yIdx),
                    4 => _cmp5.Compare(xIdx, yIdx),
                    5 => _cmp6.Compare(xIdx, yIdx),
                    6 => _cmp7.Compare(xIdx, yIdx),
                    7 => _cmp8.Compare(xIdx, yIdx),
                    _ => 0
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
                    3 => _cmp4.Compare(x, y),
                    4 => _cmp5.Compare(x, y),
                    5 => _cmp6.Compare(x, y),
                    6 => _cmp7.Compare(x, y),
                    7 => _cmp8.Compare(x, y),
                    _ => 0
                };
            }

            return cmp;
        }
    }
}
