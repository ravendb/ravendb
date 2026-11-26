using System;
using Corax.Querying.Matches.Meta;
using Corax.Utils;
using Sparrow;
using Sparrow.Compression;
using Sparrow.Server;
using Sparrow.Server.Utils;
using Voron.Data.Containers;
using Voron.Data.PostingLists;
using Voron.Impl;
using Voron.Util;

namespace Corax.Querying.Matches.TermsProviders;

/// <summary>
/// Shared posting-count machinery for the textual and numeric range providers. In-range term ids are
/// partitioned branchlessly into per-type buckets by the low two bits, then headers are read uniformly:
/// singles count as one (no container); small/large lists are sorted by container id for page locality and
/// only their header is read (varint length prefix or <see cref="PostingListState.NumberOfEntries"/>). No
/// posting ids decoded. Only the bucket-filling iteration differs per provider, so it stays in each provider;
/// the bucket lifecycle and header read live here.
/// </summary>
internal static class RangePostingBuckets
{
    // 0=Single, 1=SmallPostingList, 2=PostingList, 3=unused (kept so (termId & EnsureIsSingleMask) is always in range).
    public const int Count = 4;

    public static void Initialize(Span<NativeList<long>> buckets, ByteStringContext allocator)
    {
        for (int b = 0; b < buckets.Length; b++)
        {
            buckets[b] = new NativeList<long>();
            buckets[b].Initialize(allocator);
        }
    }

    public static void Release(Span<NativeList<long>> buckets, ByteStringContext allocator)
    {
        for (int b = 0; b < buckets.Length; b++)
            buckets[b].Dispose(allocator);
    }

    // Folds the filled buckets into the breakdown: singles need no container read; the small/large buckets have their
    // headers summed. The total postings plus the single/small/large split is the raw material the two-ended
    // range-cardinality probe extrapolates from.
    public static void Summarize(Span<NativeList<long>> buckets, ByteStringContext allocator, LowLevelTransaction llt, ref RangePostingStats stats)
    {
        if (buckets[3].Count > 0)
            throw new InvalidOperationException("Unknown TermIdMask type");

        stats.Singles = buckets[0].Count; // single = exactly one posting, no container read
        stats.SmallPostings = SumBucketPostings(buckets[1].ToSpan(), allocator, llt, isLarge: false, out stats.Smalls);
        stats.LargePostings = SumBucketPostings(buckets[2].ToSpan(), allocator, llt, isLarge: true, out stats.Larges);
        stats.Postings = stats.Singles + stats.SmallPostings + stats.LargePostings;
    }

    private static unsafe long SumBucketPostings(Span<long> termIds, ByteStringContext allocator, LowLevelTransaction llt, bool isLarge, out int count)
    {
        count = termIds.Length;
        if (count == 0)
            return 0;
        
        using var containersScope = allocator.Allocate(count, out Span<UnmanagedSpan> containers);
        EntryIdEncodings.DecodeAndDiscardFrequency(termIds, termIds.Length);
        var unique = Sorting.SortAndRemoveDuplicates(termIds);
        Container.GetAll(llt, termIds[..unique],containers, llt.PageLocator);

        return isLarge ? SumLargePostingListsCount(containers) : SumSmallPostListsCount(containers);

        long SumLargePostingListsCount(Span<UnmanagedSpan> c)
        {
            long sum = 0;
            for (int i = 0; i < c.Length; i++)
            {
                var state = (PostingListState*)c[i].Address;
                sum += state->NumberOfEntries;
            }
            return sum;
        }
        
        long SumSmallPostListsCount(Span<UnmanagedSpan> c)
        {
            long sum = 0;
            for (int i = 0; i < c.Length; i++)
            {
                sum += VariableSizeEncoding.Read<long>(c[i].Address, out _);
            }
            return sum;
        }
    }
}
