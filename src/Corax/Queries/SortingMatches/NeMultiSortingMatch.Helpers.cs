using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using Corax.Queries.SortingMatches.Comparers;
using Corax.Utils;
using Sparrow;

namespace Corax.Queries.SortingMatches;

public unsafe partial struct NewMultiSortingMatch<TInner>
{
    private static delegate*<ref NewMultiSortingMatch<TInner>, Span<long>, int> SortBy(OrderMetadata[] orderMetadata)
    {
        return (orderMetadata[0].FieldType, orderMetadata[0].Ascending) switch
        {
            (MatchCompareFieldType.Integer, true) => SortBy<EntryComparerByLong>(orderMetadata),
            (MatchCompareFieldType.Integer, false) => SortBy<Descending<EntryComparerByLong>>(orderMetadata),
            (MatchCompareFieldType.Floating, true) => SortBy<EntryComparerByDouble>(orderMetadata),
            (MatchCompareFieldType.Floating, false) => SortBy<Descending<EntryComparerByDouble>>(orderMetadata),
            (MatchCompareFieldType.Sequence, true) => SortBy<EntryComparerByTerm>(orderMetadata),
            (MatchCompareFieldType.Sequence, false) => SortBy<Descending<EntryComparerByTerm>>(orderMetadata),
            (MatchCompareFieldType.Score, true) => SortBy<EntryComparerByScore>(orderMetadata),
            (MatchCompareFieldType.Score, false) => SortBy<Descending<EntryComparerByScore>>(orderMetadata),
            (MatchCompareFieldType.Spatial, true) => SortBy<EntryComparerBySpatial>(orderMetadata),
            (MatchCompareFieldType.Spatial, false) => SortBy<Descending<EntryComparerBySpatial>>(orderMetadata),
            (MatchCompareFieldType.Alphanumeric, true) => SortBy<EntryComparerByTermAlphaNumeric>(orderMetadata),
            (MatchCompareFieldType.Alphanumeric, false) => SortBy<Descending<EntryComparerByTermAlphaNumeric>>(orderMetadata),
            _ => &Fill<NullComparer, NullComparer, NullComparer>
        };
    }

    private static delegate*<ref NewMultiSortingMatch<TInner>, Span<long>, int> SortBy<TComparer1>(OrderMetadata[] orderMetadata)
        where TComparer1 : struct, IEntryComparer, IComparer<UnmanagedSpan>
    {
        if (orderMetadata.Length == 1)
            return &Fill<TComparer1, NullComparer, NullComparer>;

        return (orderMetadata[1].FieldType, orderMetadata[1].Ascending) switch
        {
            (MatchCompareFieldType.Integer, true) => SortBy<TComparer1, EntryComparerByLong>(orderMetadata),
            (MatchCompareFieldType.Integer, false) => SortBy<TComparer1, Descending<EntryComparerByLong>>(orderMetadata),
            (MatchCompareFieldType.Floating, true) => SortBy<TComparer1, EntryComparerByDouble>(orderMetadata),
            (MatchCompareFieldType.Floating, false) => SortBy<TComparer1, Descending<EntryComparerByDouble>>(orderMetadata),
            (MatchCompareFieldType.Sequence, true) => SortBy<TComparer1, EntryComparerByTerm>(orderMetadata),
            (MatchCompareFieldType.Sequence, false) => SortBy<TComparer1, Descending<EntryComparerByTerm>>(orderMetadata),
            (MatchCompareFieldType.Spatial, true) => SortBy<TComparer1,EntryComparerBySpatial>(orderMetadata),
            (MatchCompareFieldType.Spatial, false) => SortBy<TComparer1, Descending<EntryComparerBySpatial>>(orderMetadata),
            (MatchCompareFieldType.Alphanumeric, true) => SortBy<TComparer1, EntryComparerByTermAlphaNumeric>(orderMetadata),
            (MatchCompareFieldType.Alphanumeric, false) => SortBy<TComparer1, Descending<EntryComparerByTermAlphaNumeric>>(orderMetadata),
            _ => &Fill<TComparer1, NullComparer, NullComparer>
        };
    }

    private static delegate*<ref NewMultiSortingMatch<TInner>, Span<long>, int> SortBy<TComparer1, TComparer2>(OrderMetadata[] orderMetadata)
        where TComparer1 : struct, IEntryComparer, IComparer<UnmanagedSpan>
        where TComparer2 : struct, IEntryComparer, IComparer<int>, IComparer<UnmanagedSpan>
    {
        if (orderMetadata.Length == 2)
            return &Fill<TComparer1, TComparer2, NullComparer>;

        return (orderMetadata[2].FieldType, orderMetadata[2].Ascending) switch
        {
            (MatchCompareFieldType.Integer, true) => &Fill<TComparer1, TComparer2, EntryComparerByLong>,
            (MatchCompareFieldType.Integer, false) => &Fill<TComparer1, TComparer2, Descending<EntryComparerByLong>>,
            (MatchCompareFieldType.Floating, true) => &Fill<TComparer1, TComparer2, EntryComparerByDouble>,
            (MatchCompareFieldType.Floating, false) => &Fill<TComparer1, TComparer2, Descending<EntryComparerByDouble>>,
            (MatchCompareFieldType.Sequence, true) => &Fill<TComparer1, TComparer2, EntryComparerByTerm>,
            (MatchCompareFieldType.Sequence, false) => &Fill<TComparer1, TComparer2, Descending<EntryComparerByTerm>>,
            (MatchCompareFieldType.Spatial, true) => &Fill<TComparer1, TComparer2, EntryComparerBySpatial>,
            (MatchCompareFieldType.Spatial, false) => &Fill<TComparer1, TComparer2, Descending<EntryComparerBySpatial>>,
            (MatchCompareFieldType.Alphanumeric, true) => &Fill<TComparer1, TComparer2, EntryComparerByTermAlphaNumeric>,
            (MatchCompareFieldType.Alphanumeric, false) => &Fill<TComparer1, TComparer2, Descending<EntryComparerByTermAlphaNumeric>>,
            _ => &Fill<TComparer1, TComparer2, NullComparer>
        };
    }
    
    private struct EntryComparerHelper
    {
        public static Span<int> NumericSortBatch<TComparer1, TComparer2, TComparer3>(ref NewMultiSortingMatch<TInner> match, Span<long> batchTermIds, UnmanagedSpan* batchTerms, TComparer1 comparer1, TComparer2 comparer2, TComparer3 comparer3)
            where TComparer1 : struct, IComparer<UnmanagedSpan>, IComparer<int>, IEntryComparer
            where TComparer2 : struct, IComparer<UnmanagedSpan>, IComparer<int>, IEntryComparer
            where TComparer3 : struct, IComparer<UnmanagedSpan>, IComparer<int>, IEntryComparer
        {
            var indexes = MemoryMarshal.Cast<long, int>(batchTermIds)[..(batchTermIds.Length)];
            for (int i = 0; i < batchTermIds.Length; i++)
            {
                batchTerms[i] = new UnmanagedSpan(batchTermIds[i]);
                indexes[i] = i;
            }

            IndirectSort(ref match, indexes, batchTerms, comparer1, comparer2, comparer3);

            return indexes;
        }

        public static void IndirectSort<TComparer1, TComparer2, TComparer3>(ref NewMultiSortingMatch<TInner> match, Span<int> indexes,
            UnmanagedSpan* batchTerms, TComparer1 comparer1, TComparer2 comparer2, TComparer3 comparer3)
            where TComparer1 : struct, IComparer<UnmanagedSpan>, IEntryComparer
            where TComparer2 : struct, IComparer<UnmanagedSpan>, IComparer<int>, IEntryComparer
            where TComparer3 : struct, IComparer<UnmanagedSpan>, IComparer<int>, IEntryComparer
        {
            
            if (match._orderMetadata[0].Ascending)
                indexes.Sort(new IndirectComparer<TComparer1, TComparer2, TComparer3>(ref match, batchTerms, comparer1,
                comparer2, comparer3));
            else
                indexes.Sort(new IndirectComparer<Descending<TComparer1>, TComparer2, TComparer3>(ref match, batchTerms, new(comparer1), comparer2, comparer3));

        }
    }
}
