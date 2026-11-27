using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using Corax.Indexing;
using Corax.Querying.Matches.Meta;
using Corax.Utils;
using Voron.Data.RoaringBitmaps;
using Sparrow;
using Sparrow.Compression;
using Sparrow.Server.Utils;
using Voron;
using Voron.Data.Containers;
using Voron.Data.PostingLists;
using Voron.Impl;
using Voron.Util;
using Voron.Util.PFor;

namespace Corax.Querying.Primitives;

public static class QueryPrimitives
{
    // Buffer size for stackalloc Fill operations (posting-list batch reads).
    internal const int FillBufferSize = 4096;

    // Buffer size for the posting-list AND/ANDNOT/Fill scans, intentionally great han 
    // the RoaringBitmap's array size, so we can directly jump to bitmap containers for dense ranges
    internal const int PostingScanBufferSize = 2 * FillBufferSize;

    // Bitmap slot reserved as scratch for the AND/ANDNOT primitives: The planner will never user bitmap index 1
    // so it is free for scratch usage
    public const int EphemeralBitmapSlot = 1;

    /// <summary>
    /// Synthetic posting-list ids. Both have low two bits == <see cref="TermIdMask.Reserved"/> (0b11).
    /// </summary>
    private const long EmptyPostingsId = -1;              // the term does not exist in the index (matches GetTermPostingListId's "not found" return)
    private const long AllPostingsId = long.MaxValue;     // universal source (AllIn's null-term slot when HasNullTerm=false)
    
    // Fill first clears the bitmap (unlike OR) 
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void CtxFillFromPostingSource(Matches.CompiledQueryMatch ctx, int paramIndex, int bitmapSlot)
    {
        ctx.Bitmaps[bitmapSlot].Clear();
        long postingListId = ResolvePostingListId(ref ctx.Leaves[paramIndex], ctx.Searcher, ctx.Exec);
        ctx.CutAtLimit |= FillBitmapFromPostingSource(postingListId, ctx.Searcher, ctx.Llt, ref ctx.Bitmaps[bitmapSlot], ctx.Token, ctx.OpLimit);
    }

    // Use as the complement for negation (NOT x means All Entries AND NOT x) 
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void CtxFillAllEntries(Matches.CompiledQueryMatch ctx, int bitmapSlot)
    {
        ctx.Bitmaps[bitmapSlot].Clear();
        ctx.CutAtLimit |= OrWithMatch(ctx.Searcher.AllEntries(), ref ctx.Bitmaps[bitmapSlot], ctx.OpLimit);
    }

    public static void CtxSaveScoringGroup(Matches.CompiledQueryMatch ctx, int bitmapSlot, int groupIndex)
    {
        ctx.ScoringGroups[groupIndex] = ctx.Bitmaps[bitmapSlot].Clone();
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void CtxFillFromTreeScan(Matches.CompiledQueryMatch ctx, int paramIndex, int bitmapSlot)
    {
        ctx.Bitmaps[bitmapSlot].Clear();
        var (tally, cut) = FillBitmapFromTreeScan(ResolveTermsProvider(ref ctx.Leaves[paramIndex], ctx.Searcher, ctx.Exec), ctx.Llt, ref ctx.Bitmaps[bitmapSlot], ctx.Token, ctx.OpLimit);
        ctx.CutAtLimit |= cut;
        if (ctx.OpLimit != long.MaxValue) 
            return; // we cannot observe the tree scan if we didn't complete it (each posting list is cut at the set limit).
        ObserveTreeScanTally(ref ctx.Leaves[paramIndex], tally);
    }

    // Feed a tree-scan fill's actual count into EWMA range calibration, so next queries we'll be smarter
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void ObserveTreeScanTally(ref Planning.LeafResolveInfo leaf, long tally)
    {
        if (tally >= 0 && leaf.RangeCalibration != null)
            leaf.RangeCalibration.Observe(tally, leaf.RangeEstimate);
    }

    // An OR operand is cut to its own first OpLimit ids: the union of these holds the first OpLimit ids of the whole union
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void CtxOrFillFromPostingSource(Matches.CompiledQueryMatch ctx, int paramIndex, int bitmapSlot)
    {
        long postingListId = ResolvePostingListId(ref ctx.Leaves[paramIndex], ctx.Searcher, ctx.Exec);
        ctx.CutAtLimit |= FillBitmapFromPostingSource(postingListId, ctx.Searcher, ctx.Llt, ref ctx.Bitmaps[bitmapSlot], ctx.Token, ctx.OpLimit);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void CtxOrFillFromTreeScan(Matches.CompiledQueryMatch ctx, int paramIndex, int bitmapSlot)
    {
        ctx.CutAtLimit |= FillBitmapFromTreeScan(ResolveTermsProvider(ref ctx.Leaves[paramIndex], ctx.Searcher, ctx.Exec), ctx.Llt, ref ctx.Bitmaps[bitmapSlot], ctx.Token, ctx.OpLimit).Cut;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void CtxOrWithMatchSlot(Matches.CompiledQueryMatch ctx, int paramIndex, int bitmapSlot)
    {
        ctx.CutAtLimit |= OrWithMatch(ctx.ResolvedMatches[paramIndex], ref ctx.Bitmaps[bitmapSlot], ctx.OpLimit, ctx.Token, ctx.PreserveLeavesForScoring);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void CtxFillFromMatch(Matches.CompiledQueryMatch ctx, int paramIndex, int bitmapSlot)
    {
        ctx.Bitmaps[bitmapSlot].Clear();
        ctx.CutAtLimit |= OrWithMatch(ctx.ResolvedMatches[paramIndex], ref ctx.Bitmaps[bitmapSlot], ctx.OpLimit, ctx.Token, ctx.PreserveLeavesForScoring);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void CtxAndFromPostingSource(Matches.CompiledQueryMatch ctx, int paramIndex, int bitmapSlot)
    {
        Debug.Assert(bitmapSlot != EphemeralBitmapSlot, "AND destination must not alias the AND scratch slot.");
        long postingListId = ResolvePostingListId(ref ctx.Leaves[paramIndex], ctx.Searcher, ctx.Exec);
        ctx.CutAtLimit |= AndWithPostingSource(postingListId, ctx.Searcher, ctx.Llt, ref ctx.Bitmaps[bitmapSlot], ref ctx.Bitmaps[EphemeralBitmapSlot], ctx.Token, ctx.OpLimit);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void CtxAndFromTreeScan(Matches.CompiledQueryMatch ctx, int paramIndex, int bitmapSlot)
    {
        Debug.Assert(bitmapSlot != EphemeralBitmapSlot, "AND destination must not alias the AND scratch slot.");
        long tally = AndBitmapWithTreeScan(ResolveTermsProvider(ref ctx.Leaves[paramIndex], ctx.Searcher, ctx.Exec), ctx.Llt, ref ctx.Bitmaps[bitmapSlot], ref ctx.Bitmaps[EphemeralBitmapSlot], ctx.Token);
        ObserveTreeScanTally(ref ctx.Leaves[paramIndex], tally);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void CtxAndFromMatch(Matches.CompiledQueryMatch ctx, int paramIndex, int bitmapSlot)
    {
        Debug.Assert(bitmapSlot != EphemeralBitmapSlot, "AND destination must not alias the AND scratch slot.");
        AndWithMatch(ctx.ResolvedMatches[paramIndex], ref ctx.Bitmaps[bitmapSlot], ref ctx.Bitmaps[EphemeralBitmapSlot], ctx.Token, ctx.PreserveLeavesForScoring);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void CtxAndNotFromPostingSource(Matches.CompiledQueryMatch ctx, int paramIndex, int bitmapSlot)
    {
        Debug.Assert(bitmapSlot != EphemeralBitmapSlot, "ANDNOT destination must not alias the AND scratch slot.");
        long postingListId = ResolvePostingListId(ref ctx.Leaves[paramIndex], ctx.Searcher, ctx.Exec);
        AndNotWithPostingSource(postingListId, ctx.Searcher, ctx.Llt, ref ctx.Bitmaps[bitmapSlot], ref ctx.Bitmaps[EphemeralBitmapSlot], ctx.Token);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void CtxAndNotFromTreeScan(Matches.CompiledQueryMatch ctx, int paramIndex, int bitmapSlot)
    {
        Debug.Assert(bitmapSlot != EphemeralBitmapSlot, "ANDNOT destination must not alias the AND scratch slot.");
        long tally = AndNotBitmapWithTreeScan(ResolveTermsProvider(ref ctx.Leaves[paramIndex], ctx.Searcher, ctx.Exec), ctx.Llt, ref ctx.Bitmaps[bitmapSlot], ref ctx.Bitmaps[EphemeralBitmapSlot], ctx.Token);
        ObserveTreeScanTally(ref ctx.Leaves[paramIndex], tally);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void CtxAndNotFromMatch(Matches.CompiledQueryMatch ctx, int paramIndex, int bitmapSlot)
    {
        Debug.Assert(bitmapSlot != EphemeralBitmapSlot, "ANDNOT destination must not alias the AND scratch slot.");
        AndNotWithMatch(ctx.ResolvedMatches[paramIndex], ref ctx.Bitmaps[bitmapSlot], ref ctx.Bitmaps[EphemeralBitmapSlot], ctx.Token, ctx.PreserveLeavesForScoring);
    }

    /// <summary>Resolve a posting-list leaf slot to its raw posting-list id, or a synthetic sentinel
    /// (<see cref="EmptyPostingsId"/> / <see cref="AllPostingsId"/>) for the non-term slots.</summary>
    private static long ResolvePostingListId(ref Planning.LeafResolveInfo info, IndexSearcher searcher, Planning.QueryExecution exec)
    {
        return info.Kind switch
        {
            // GetTermPostingListId already returns -1 (== EmptyPostingsId) when the term doesn't exist.
            Planning.LeafResolveKind.TermPosting => info.Packed.GetTermPostingListId(info.FieldMeta, searcher, exec),
            Planning.LeafResolveKind.NullPosting => searcher.TryGetPostingListForNull(in info.FieldMeta, out long nullPlId) ? nullPlId : EmptyPostingsId,
            Planning.LeafResolveKind.AllPosting => AllPostingsId,
            _ => EmptyPostingsId
        };
    }

    /// <summary>Materialize the <see cref="ITermsProvider"/> for a tree-scan leaf slot.</summary>
    private static ITermsProvider ResolveTermsProvider(ref Planning.LeafResolveInfo info, IndexSearcher searcher, Planning.QueryExecution exec)
    {
        IQueryMatch match = info.ClauseType switch
        {
            Planning.ClauseType.Exists => searcher.ExistsQuery(info.FieldMeta),
            Planning.ClauseType.StartsWith => searcher.StartWithQuery(info.FieldMeta, exec.StringValues[info.Packed.Param1]),
            Planning.ClauseType.EndsWith => searcher.EndsWithQuery(info.FieldMeta, exec.StringValues[info.Packed.Param1]),
            Planning.ClauseType.Regex => searcher.RegexQuery(info.FieldMeta, exec.RegexFactory(exec.StringValues[info.Packed.Param1])),
            Planning.ClauseType.GreaterThan or Planning.ClauseType.GreaterThanOrEqual
                or Planning.ClauseType.LessThan or Planning.ClauseType.LessThanOrEqual
                => info.Packed.RangeQuery(info.ClauseType, info.FieldMeta, searcher, exec),
            Planning.ClauseType.Between => info.Packed.BetweenQuery(info.FieldMeta, searcher, exec),
            _ => null
        };

        return match is Matches.TermsProviderMatch tpm ? tpm.Provider : EmptyTermsProvider.Instance;
    }

    // Batch size for entry scan: how many bitmap entries to read per iteration.
    internal const int EntryScanBatchSize = 256;

    public const int TieBreakGroupInitialCapacity = 1024;

    // Entry scan vs. bitmap AND heuristic (tuned on typical NVMe workloads): When the candidate bitmap is small
    // enough, it's cheaper to read each entry's stored fields and check predicates than to decode a full posting list and AND.
    public const long EntryScanCountThreshold = 32 * 1024;

    // Approximate cost ratio: one entry blob read (EntryTermsReader stored-field fetch + residual check) vs a
    // single posting-list decode. Entry scan wins when entriesToScan * multiplier < bitmapCost.
    // Works alongside: EntryScanSurvivorSortFactor
    public const long EntryScanCostMultiplier = 128;

    // The bitmap pipeline doesn't just decode posting lists (Σ cardinalities) — it then SORTS the surviving
    // intersection. bitmapCost = Σ cardinalities + survivors × EntryScanSurvivorSortFactor, survivors estimated
    // by the independence (product) rule. Without this term the cost is regime-blind (few survivors -> cheap
    // bitmap; many -> sort-bound). The factor approximates sort+materialize-one-survivor in posting-decode units.
    public const long EntryScanSurvivorSortFactor = 32;

    // Sentinels for the $rvn_corax_entry_scan override carried on ForcedEntryScanGate.
    // Unset leaves the cost gate in charge; setting to a negative number disable this entirely
    public const int EntryScanGateUnset = int.MinValue;

    /// <returns>True when the limit cut entries off (the list goes on past its first <paramref name="limit"/> ids).</returns>
    [SkipLocalsInit]
    private static bool FillFromPostings(ref PostingList.Iterator iterator, ref RoaringBitmap bitmap, CancellationToken token, long limit = long.MaxValue)
    {
        Span<long> buffer = stackalloc long[PostingScanBufferSize];

        long total = 0;
        // one entry past the limit is enough to tell that the list goes on
        while (iterator.Fill(buffer[..((int)Math.Min(buffer.Length - 1, limit - total) + 1)], out int read) && read > 0)
        {
            token.ThrowIfCancellationRequested();
            bool cut = read > limit - total;
            if (cut)
                read = (int)(limit - total);
            EntryIdEncodings.DecodeAndDiscardFrequency(buffer, read);
            bitmap.AddRange(buffer[..read]);
            if (cut)
                return true;
            total += read;
        }

        return false;
    }

    /// <summary>
    /// Only posting-list pages overlapping the bitmap's entry-id range are touched (a 50K bitmap vs a 10M posting list
    /// reads only the pages covering the 50K range). 
    /// </summary>
    private static bool TrySetupPostingScan(ref PostingList.Iterator iterator, ref RoaringBitmap bitmap, out long pruneAfter)
    {
        pruneAfter = 0;
        if (bitmap.IsEmpty)
            return false;
        
        // Bound the posting list scan to the bitmap's container key range.
        long minKey = bitmap.MinContainerKey;
        long maxKey = bitmap.MaxContainerKey;
        Debug.Assert(minKey is not -1 && maxKey is not -1, "shouldn't happen, we checked IsEmpty");

        // Encode to posting-list space: the posting list stores encoded values (entryId << 10 | freq | type),
        // so Seek and Fill both expect encoded bounds, not raw decoded entry IDs.
        long seekFrom = EntryIdEncodings.PrepareIdForSeekInPostingList(minKey * RoaringBitmap.ContainerSize);
        pruneAfter = EntryIdEncodings.PrepareIdForPruneInPostingList((maxKey + 1) * RoaringBitmap.ContainerSize - 1);

        // Seek past all posting list entries below the bitmap's range
        if (iterator.Seek(seekFrom)) 
            return true;
        
        bitmap.Clear(); // no matches to AND with
        return false;
    }

    [SkipLocalsInit]
    private static void AndWithPostings(ref PostingList.Iterator iterator, ref RoaringBitmap bitmap, ref RoaringBitmap tempBitmap, CancellationToken token)
    {
        if (TrySetupPostingScan(ref iterator, ref bitmap, out long pruneAfter) is false)
            return;

        tempBitmap.Clear();
        Span<long> buffer = stackalloc long[PostingScanBufferSize];
        while (iterator.Fill(buffer, out int read, pruneAfter) && read > 0)
        {
            token.ThrowIfCancellationRequested();
            EntryIdEncodings.DecodeAndDiscardFrequency(buffer, read);
            tempBitmap.AddRange(buffer[..read]);
        }

        bitmap.AndWith(ref tempBitmap);
    }

    /// <summary>
    /// Limit-aware AND for unordered "limit N" queries: keeps every survivor below the first container past N survivors,
    /// so the first N survivors by entry id are kept.
    /// </summary>
    [SkipLocalsInit]
    private static bool AndWithPostingsLimited(ref PostingList.Iterator iterator, ref RoaringBitmap bitmap, ref RoaringBitmap tempBitmap, CancellationToken token, long limit)
    {
        if (TrySetupPostingScan(ref iterator, ref bitmap, out long pruneAfter) is false)
            return false;

        tempBitmap.Clear();
        Span<long> buffer = stackalloc long[PostingScanBufferSize];
        long matched = 0;
        int processedKey = 0;
        while (iterator.Fill(buffer, out int read, pruneAfter) && read > 0)
        {
            token.ThrowIfCancellationRequested();
            EntryIdEncodings.DecodeAndDiscardFrequency(buffer, read);
            tempBitmap.AddRange(buffer[..read]);

            int seenMaxKey = (int)(buffer[read - 1] >> RoaringBitmap.ContainerKeyShift);
            matched += bitmap.AndWithRange(ref tempBitmap, processedKey, seenMaxKey); // half-open [processedKey, seenMaxKey): excludes the latest container, which may still grow next batch
            processedKey = seenMaxKey;

            if (matched < limit) continue;
            
            // enough survivors already; drop the unscanned tail
            bool cut = bitmap.MaxContainerKey >= processedKey;
            bitmap.RemoveContainersFrom(processedKey);
            return cut;
        }

        bitmap.AndWithRange(ref tempBitmap, processedKey, int.MaxValue); // term complete: finish the tail
        return false;
    }

    /// <summary>
    /// ANDNOT the bitmap with a posting list. Same bounded range scan as AndWith —
    /// only reads posting list pages that overlap with the bitmap's container range.
    /// </summary>
    [SkipLocalsInit]
    private static void AndNotWithPostings(ref PostingList.Iterator iterator, ref RoaringBitmap bitmap, ref RoaringBitmap tempBitmap, CancellationToken token)
    {
        if (bitmap.IsEmpty)
            return;

        tempBitmap.Clear();

        long minKey = bitmap.MinContainerKey;
        long maxKey = bitmap.MaxContainerKey;
        Debug.Assert(minKey is not -1 && maxKey is not -1, "shouldn't happen, we checked IsEmpty");

        long seekFrom = EntryIdEncodings.PrepareIdForSeekInPostingList(minKey * RoaringBitmap.ContainerSize);
        long pruneAfter = EntryIdEncodings.PrepareIdForPruneInPostingList((maxKey + 1) * RoaringBitmap.ContainerSize - 1);

        if (!iterator.Seek(seekFrom))
            return; // No entries in range — nothing to subtract

        Span<long> buffer = stackalloc long[PostingScanBufferSize];
        while (iterator.Fill(buffer, out int read, pruneAfter) && read > 0)
        {
            token.ThrowIfCancellationRequested();
            EntryIdEncodings.DecodeAndDiscardFrequency(buffer, read);
            tempBitmap.AddRange(buffer[..read]);
        }

        bitmap.AndNotWith(ref tempBitmap);
    }

    /// <summary>
    /// Runtime check: should we switch from bitmap AND to per-entry scan? Called directly from IL-emitted code.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool ShouldSwitchToEntryScan(int forcedGate, int gate, long bitmapCount, long nextClauseCardinality)
    {
        // A forced gate (set via $rvn_corax_entry_scan) is EXCLUSIVE: disabling the heuristic below
        if (forcedGate != EntryScanGateUnset)
            return forcedGate == gate;

        return bitmapCount < EntryScanCountThreshold && bitmapCount * EntryScanCostMultiplier < nextClauseCardinality;
    }

    /// <summary>Fill bitmap from an IQueryMatch by calling Fill repeatedly.
    /// Fast paths (consume-after-use semantics — sources are not read again):
    ///   - IBitmapQueryMatch: steal containers via LazyOrWith + one RepairAfterLazy pass.
    ///   - TermMatch backed by a large posting list: native FillFromPostings on the iterator,
    ///     skipping the per-batch IQueryMatch + function-pointer indirection.</summary>
    /// <returns>True when the limit cut entries off.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    [SkipLocalsInit]
    public static bool OrWithMatch(IQueryMatch match, ref RoaringBitmap bitmap, long limit = long.MaxValue, CancellationToken token = default, bool preserveLeaf = false)
    {
        if (match is IBitmapQueryMatch bm)
        {
            // Limit is intentionally ignored here: the source bitmap is already fully
            // materialized, so there's no I/O to save. Truncating would break Count
            // (used for TotalResults) and Contains (used by sorting).
            ref RoaringBitmap srcData = ref bm.BitmapState;
            if (srcData.IsEmpty)
                return false;
            if (preserveLeaf)
            {
                // Score-sorted query: OrWith would steal this leaf's unique containers and mark it consumed,
                // but the score pass re-reads it afterwards. Fold a clone instead so the leaf stays intact.
                var clone = srcData.Clone();
                bitmap.OrWith(ref clone);
                clone.Dispose(); // OrWith stole only unique containers (detached from the clone); the clone still owns any shared ones, so dispose to release them.
                return false;
            }
            bitmap.OrWith(ref srcData);
            return false;
        }
        // A term whose relevance is stored must go through Fill: that is where TermMatch hands ids and frequencies
        // to Bm25Relevance. Bigger posting lists are re-read at score time, so they keep the fast path.
        if (match is Matches.TermMatch tm && tm.ScoringNeedsFill == false && tm.TryGetPostingListIterator(out var iter))
        {
            return FillFromPostings(ref iter, ref bitmap, token, limit);
        }

        // Fill sorts within one call only, so a cut keeps the first ids only of a source sorted across calls
        if (match is not Matches.AllEntriesMatch)
            limit = long.MaxValue;

        Span<long> buffer = stackalloc long[FillBufferSize];
        int read;
        long total = 0;
        while ((read = match.Fill(buffer)) > 0)
        {
            token.ThrowIfCancellationRequested();
            bool cut = read > limit - total;
            if (cut)
                read = (int)(limit - total);
            bitmap.AddRange(buffer.Slice(0, read));
            if (cut)
                return true;
            total += read;
        }

        return false;
    }

    /// <summary>Fill temp bitmap from match, then AND with target.
    /// Fast paths:
    ///   - Match exposes a RoaringBitmap (IBitmapQueryMatch): AND in place against the borrowed bitmap.
    ///   - Match is a TermMatch backed by a large posting list: use the bounded range scan
    ///     <see cref="AndWithPostings"/>, which bounds the posting-list scan to the bitmap's
    ///     container range — only reads pages that can intersect, instead of materializing
    ///     the full posting list into a temp bitmap.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    [SkipLocalsInit]
    public static void AndWithMatch(IQueryMatch match, ref RoaringBitmap bitmap, ref RoaringBitmap tempBitmap, CancellationToken token = default, bool preserveLeaf = false)
    {
        if (match is Matches.AllEntriesMatch)
            return;

        if (match is IBitmapQueryMatch bm)
        {
            ref RoaringBitmap srcData = ref bm.BitmapState;
            if (preserveLeaf)
            {
                // Score-sorted query: AndWith consumes the leaf (right side). Fold a clone so the leaf stays
                // intact for the score pass; the result lands in `bitmap` exactly as before.
                var clone = srcData.Clone();
                bitmap.AndWith(ref clone);
                clone.Dispose();
                return;
            }
            bitmap.AndWith(ref srcData);
            return;
        }
        if (match is Matches.TermMatch tm && tm.ScoringNeedsFill == false && tm.TryGetPostingListIterator(out var iter))
        {
            AndWithPostings(ref iter, ref bitmap, ref tempBitmap, token);
            return;
        }
        tempBitmap.Clear();
        OrWithMatch(match, ref tempBitmap, token: token);
        bitmap.AndWith(ref tempBitmap);
    }

    /// <summary>Fill temp bitmap from match, then ANDNOT from target.
    /// Fast paths mirror <see cref="AndWithMatch"/> — bitmap-borrow for IBitmapQueryMatch,
    /// bounded range scan <see cref="AndNotWithPostings"/> for TermMatch with a large posting list.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    [SkipLocalsInit]
    public static void AndNotWithMatch(IQueryMatch match, ref RoaringBitmap bitmap, ref RoaringBitmap tempBitmap, CancellationToken token = default, bool preserveLeaf = false)
    {
        if (match is IBitmapQueryMatch bm)
        {
            ref RoaringBitmap srcData = ref bm.BitmapState;
            if (preserveLeaf)
            {
                // Score-sorted query: AndNotWith consumes the leaf (right side). Fold a clone so the leaf stays
                // intact for the score pass; the result lands in `bitmap` exactly as before.
                var clone = srcData.Clone();
                bitmap.AndNotWith(ref clone);
                clone.Dispose();
                return;
            }
            bitmap.AndNotWith(ref srcData);
            return;
        }
        // A negated leaf is resolved without boost (see QueryPlanBuilder.ResolveFieldMetadata), so it never feeds BM25 and never needs Fill.
        if (match is Matches.TermMatch tm && tm.TryGetPostingListIterator(out var iter))
        {
            AndNotWithPostings(ref iter, ref bitmap, ref tempBitmap, token);
            return;
        }
        tempBitmap.Clear();
        OrWithMatch(match, ref tempBitmap, token: token);
        bitmap.AndNotWith(ref tempBitmap);
    }

    /// <summary>OR a posting-list source into the bitmap. The low two bits of the id select the source type
    /// (Single → Add; SmallPostingList → decode FastPFor buffer + AddRange; PostingList → <see cref="FillFromPostings"/>).
    /// A synthetic id (NotARealValue bucket) can only be Empty here → no-op.</summary>
    /// <returns>True when the limit cut entries off.</returns>
    [SkipLocalsInit]
    private static bool FillBitmapFromPostingSource(
        long postingListId,
        IndexSearcher searcher,
        LowLevelTransaction llt,
        ref RoaringBitmap bitmap,
        CancellationToken token,
        long limit = long.MaxValue)
    {
        switch ((TermIdMask)postingListId & TermIdMask.EnsureIsSingleMask)
        {
            case TermIdMask.Single:
                if (limit > 0)
                    bitmap.Add((long)EntryIdEncodings.GetContainerId(postingListId));
                return false;

            case TermIdMask.SmallPostingList:
                return AddSmallPostingListToBitmap(llt, (long)EntryIdEncodings.GetContainerId(postingListId), ref bitmap, token, limit);

            case TermIdMask.PostingList:
                var iterator = searcher.GetPostingList(postingListId).Iterate();
                return FillFromPostings(ref iterator, ref bitmap, token, limit);

            case  TermIdMask.Reserved when postingListId == EmptyPostingsId:
                return false; // nothing to do
                
            default:
                throw new ArgumentOutOfRangeException(nameof(postingListId), "All posting source is not expected on a fill path.");
        }
    }

    /// <summary>AND the bitmap with a posting-list source. Bounded range scan when the source is a large
    /// PostingList; per-key membership / temp-bitmap-fill for the smaller cases. A synthetic id is either
    /// Empty (clears the bitmap — intersection with nothing) or All (universal pass-through — no-op).</summary>
    /// <returns>True when the limit cut survivors off.</returns>
    [SkipLocalsInit]
    private static bool AndWithPostingSource(
        long postingListId,
        IndexSearcher searcher,
        LowLevelTransaction llt,
        ref RoaringBitmap bitmap,
        ref RoaringBitmap tempBitmap,
        CancellationToken token,
        long limit = long.MaxValue)
    {
        if (bitmap.IsEmpty)
            return false;

        switch ((TermIdMask)postingListId & TermIdMask.EnsureIsSingleMask)
        {
            case TermIdMask.Single:
            {
                long entryId = (long)EntryIdEncodings.GetContainerId(postingListId);
                bool keep = bitmap.Contains(entryId);
                bitmap.Clear();
                if (keep)
                    bitmap.Add(entryId);
                return false;
            }

            case TermIdMask.SmallPostingList:
                // not limiting the small posting list, since we want to limit the AND result, not the source data 
                MaterializeTermSourceIntoBitmap(postingListId, llt, ref tempBitmap, token);
                bitmap.AndWith(ref tempBitmap);
                return false;

            case TermIdMask.PostingList:
                var iterator = searcher.GetPostingList(postingListId).Iterate();
                return AndWithPostingsLimited(ref iterator, ref bitmap, ref tempBitmap, token, limit);

            case TermIdMask.Reserved when postingListId == AllPostingsId:
                return false;
            case TermIdMask.Reserved when postingListId == EmptyPostingsId:
                bitmap.Clear();
                return false;
        }

        return false;
    }

    /// <summary>ANDNOT the bitmap with a posting-list source (subtract). A synthetic id can only be Empty
    /// here → no-op (subtracting nothing).</summary>
    [SkipLocalsInit]
    private static void AndNotWithPostingSource(
        long postingListId,
        IndexSearcher searcher,
        LowLevelTransaction llt,
        ref RoaringBitmap bitmap,
        ref RoaringBitmap tempBitmap,
        CancellationToken token)
    {
        if (bitmap.IsEmpty)
            return;

        switch ((TermIdMask)postingListId & TermIdMask.EnsureIsSingleMask)
        {
            case TermIdMask.Single:
            case TermIdMask.SmallPostingList:
                MaterializeTermSourceIntoBitmap(postingListId, llt, ref tempBitmap, token);
                bitmap.AndNotWith(ref tempBitmap);
                return;

            case TermIdMask.PostingList:
                var iterator = searcher.GetPostingList(postingListId).Iterate();
                AndNotWithPostings(ref iterator, ref bitmap, ref tempBitmap, token);
                return;
            
            case  TermIdMask.Reserved when postingListId == EmptyPostingsId:
                return; // nothing to do
                
            default:
                throw new ArgumentOutOfRangeException(nameof(postingListId), "All posting source is not expected on an ANDNOT path.");
        }
    }

    /// <summary>Materialize a Single or SmallPostingList source into the temp bitmap (clears it first).
    /// Shared by AndWithPostingSource and AndNotWithPostingSource to avoid duplicating the
    /// clear-then-populate pattern for these small-source cases.</summary>
    private static void MaterializeTermSourceIntoBitmap(
        long postingListId,
        LowLevelTransaction llt,
        ref RoaringBitmap tempBitmap,
        CancellationToken token,
        long limit = long.MaxValue)
    {
        tempBitmap.Clear();
        switch ((TermIdMask)postingListId & TermIdMask.EnsureIsSingleMask)
        {
            case TermIdMask.Single:
                tempBitmap.Add((long)EntryIdEncodings.GetContainerId(postingListId));
                return;
            case TermIdMask.SmallPostingList:
                AddSmallPostingListToBitmap(llt, (long)EntryIdEncodings.GetContainerId(postingListId), ref tempBitmap, token, limit);
                return;
            default:
                throw new ArgumentOutOfRangeException($"MaterializeTermSourceIntoBitmap called with unexpected id: {postingListId}");
        }
    }

    /// <summary>Fetch the small posting list container by id, decode the
    /// FastPFor stream into the bitmap. Allocates a stackalloc buffer +
    /// FastPForBufferedReader scoped to this call.</summary>
    [SkipLocalsInit]
    private static unsafe bool AddSmallPostingListToBitmap(
        LowLevelTransaction llt,
        long smallPostingListId,
        ref RoaringBitmap bitmap,
        CancellationToken token,
        long limit = long.MaxValue)
    {
        Container.Get(llt, (ContainerEntryId)smallPostingListId, out var item);
        _ = VariableSizeEncoding.Read<int>(item.Address, out var offset);

        var buffer = stackalloc long[FillBufferSize];
        using var reader = new FastPForBufferedReader(llt.Allocator);
        reader.Init(item.Address + offset, item.Length - offset);
        int read;
        long total = 0;
        while ((read = reader.Fill(buffer, FillBufferSize)) > 0)
        {
            token.ThrowIfCancellationRequested();
            bool cut = read > limit - total;
            if (cut)
                read = (int)(limit - total);
            var results = new Span<long>(buffer, read);
            EntryIdEncodings.DecodeAndDiscardFrequency(results, read);
            bitmap.AddRange(results[..read]);
            if (cut)
                return true;
            total += read;
        }

        return false;
    }

    /// <summary>
    /// Fill a bitmap by walking an ITermsProvider's posting list IDs in batches.
    /// Each batch is partitioned into three buckets keyed by TermIdMask:
    ///   - Single: container ID strip + sort/dedup, then bitmap.AddRange.
    ///   - SmallPostingList: container ID strip + sort/dedup, batch Container.GetAll,
    ///     decode each posting list inline via FastPForBufferedReader.
    ///   - PostingList: container ID strip + sort/dedup, then iterate each via FillFromPostings.
    /// Partitioning is branchless: (id &amp; EnsureIsSingleMask) yields the bucket index.
    /// A <paramref name="limit"/> cuts each term's posting list to its first ids, never the term walk: terms come in term
    /// order, not entry id order, and the union of the cut lists still holds the first <paramref name="limit"/> ids of the scan.
    /// </summary>
    /// <returns>The over-counting postings tally: the running sum of posting-list sizes fed into the
    /// bitmap (singles + small + large bucket <c>PostingListState.NumberOfEntries</c>), counting
    /// multi-valued documents once per matching term. This is exactly the quantity
    /// <c>EstimateMatchesInRange</c> predicts, so callers can Observe(tally, estimate) to calibrate it —
    /// but only when the fill was unbounded (<paramref name="limit"/> == long.MaxValue).</returns>
    [SkipLocalsInit]
    public static unsafe (long Tally, bool Cut) FillBitmapFromTreeScan(
        ITermsProvider provider,
        LowLevelTransaction llt,
        ref RoaringBitmap bitmap,
        CancellationToken token = default,
        long limit = long.MaxValue)
    {
        Span<long> plIds = stackalloc long[FillBufferSize];
        Span<long> entryBuffer = stackalloc long[FillBufferSize];

        // Branchless partition: index by (id & EnsureIsSingleMask) yields 0..3.
        // 0=Single, 1=SmallPostingList, 2=PostingList. Slot 3 is unused (mask 0b11) -
        // we keep it so indexing is safe and validate it stays empty.
        Span<NativeList<long>> buckets = stackalloc NativeList<long>[4];
        for (int b = 0; b < buckets.Length; b++)
        {
            buckets[b] = new NativeList<long>();
            buckets[b].Initialize(llt.Allocator, FillBufferSize);
        }

        var pageLocator = llt.PageLocator;

        var containerItems = new ContextBoundNativeList<UnmanagedSpan>(llt.Allocator, FillBufferSize);
        FastPForBufferedReader smallListReader = default;
        bool readerInitialized = false;

        // an upper bound on the real cardinality (duplicates across terms collapse on insert).
        long upperBound = 0;
        bool cut = false;
        try
        {
            int read;
            while ((read = provider.FillPostingListIds(plIds, termLengths: Span<int>.Empty)) > 0)
            {
                token.ThrowIfCancellationRequested();
                for (int b = 0; b < buckets.Length; b++)
                    buckets[b].Clear();

                // Branchless partition - capacity reserved up front, AddUnsafe is safe
                for (int i = 0; i < read; i++)
                {
                    var pid = plIds[i];
                    int idx = (int)(pid & (long)TermIdMask.EnsureIsSingleMask);
                    buckets[idx].AddUnsafe(pid);
                }

                if (buckets[3].Count > 0)
                    throw new InvalidOperationException("Unknown TermIdMask type");

                // Bucket 0: Single -> strip frequency first so dedup is keyed on the entry id
                var singlesSpan = buckets[0].ToSpan();
                if (singlesSpan.Length > 0)
                {
                    EntryIdEncodings.DecodeAndDiscardFrequency(singlesSpan, singlesSpan.Length);
                    var singlesLen = Sorting.SortAndRemoveDuplicates(singlesSpan);
                    bitmap.AddRange(singlesSpan[..singlesLen]);
                    upperBound += singlesLen;
                }

                // Bucket 1: SmallPostingList -> strip frequency, dedup, batch fetch, decode
                var smallsSpan = buckets[1].ToSpan();
                if (smallsSpan.Length > 0)
                {
                    EntryIdEncodings.DecodeAndDiscardFrequency(smallsSpan, smallsSpan.Length);
                    var smallLen = Sorting.SortAndRemoveDuplicates(smallsSpan);

                    containerItems.Clear();
                    containerItems.EnsureCapacityFor(smallLen);
                    containerItems.Count = smallLen;
                    Container.GetAll(llt, smallsSpan[..smallLen], containerItems.ToSpan(), pageLocator);

                    if (readerInitialized == false)
                    {
                        smallListReader = new FastPForBufferedReader(llt.Allocator);
                        readerInitialized = true;
                    }

                    fixed (long* pEntryBuffer = entryBuffer)
                    {
                        for (int i = 0; i < smallLen; i++)
                        {
                            var item = containerItems[i];
                            _ = VariableSizeEncoding.Read<int>(item.Address, out var offset);
                            smallListReader.Init(item.Address + offset, item.Length - offset);

                            int smallRead;
                            long left = limit;
                            while ((smallRead = smallListReader.Fill(pEntryBuffer, (int)Math.Min(entryBuffer.Length - 1, left) + 1)) > 0)
                            {
                                token.ThrowIfCancellationRequested();
                                EntryIdEncodings.DecodeAndDiscardFrequency(entryBuffer, smallRead);
                                upperBound += smallRead;
                                if (smallRead > left)
                                {
                                    bitmap.AddRange(entryBuffer[..(int)left]);
                                    cut = true;
                                    break;
                                }

                                bitmap.AddRange(entryBuffer[..smallRead]);
                                left -= smallRead;
                            }
                        }
                    }
                }

                // Bucket 2: PostingList -> strip frequency, dedup, then iterate each
                var largeSpan = buckets[2].ToSpan();
                if (largeSpan.Length > 0)
                {
                    EntryIdEncodings.DecodeAndDiscardFrequency(largeSpan, largeSpan.Length);
                    var largeLen = Sorting.SortAndRemoveDuplicates(largeSpan);
                    for (int i = 0; i < largeLen; i++)
                    {
                        var setStateSpan = Container.GetReadOnly(llt, new ContainerEntryId(largeSpan[i]));
                        ref readonly var setState = ref MemoryMarshal.AsRef<PostingListState>(setStateSpan);
                        using var postingList = new PostingList(llt, Slices.Empty, in setState);
                        var iterator = postingList.Iterate();
                        cut |= FillFromPostings(ref iterator, ref bitmap, token, limit);

                        // FillFromPostings doesn't tell us the exact count it added, so we use NumberOfEntries
                        // to keep upperBound an over-counting tally. May be less than that if there are duplicates, but not less 
                        upperBound += setState.NumberOfEntries;
                    }
                }
            }
        }
        finally
        {
            if (readerInitialized)
                smallListReader.Dispose();
            containerItems.Dispose();
            for (int b = 0; b < buckets.Length; b++)
                buckets[b].Dispose(llt.Allocator);
        }

        return (upperBound, cut);
    }

    /// <summary>AND the bitmap with the union of all posting lists produced by the term provider.
    /// Fills a scratch bitmap from the provider, then ANDs the result bitmap with it.
    /// If the provider produces no matches, the bitmap is cleared.</summary>
    /// <returns>The scratch fill's over-counting postings tally (the scratch fill is always unbounded,
    /// so it is always complete and calibration-grade), or -1 when the bitmap was already empty and no
    /// fill ran — the caller must not Observe on -1.</returns>
    private static long AndBitmapWithTreeScan(
        ITermsProvider provider,
        LowLevelTransaction llt,
        ref RoaringBitmap bitmap,
        ref RoaringBitmap tempBitmap,
        CancellationToken token)
    {
        if (bitmap.IsEmpty)
            return -1;
        tempBitmap.Clear();
        long tally = FillBitmapFromTreeScan(provider, llt, ref tempBitmap, token).Tally;
        if (tempBitmap.IsEmpty)
        {
            bitmap.Clear();
            return tally;
        }
        bitmap.AndWith(ref tempBitmap);
        return tally;
    }

    /// <summary>ANDNOT the bitmap with the union of all posting lists produced by the term provider
    /// (subtract matching entries). If the provider produces no matches, the bitmap is unchanged.</summary>
    /// <returns>The scratch fill's over-counting postings tally (always unbounded, hence
    /// calibration-grade), or -1 when the bitmap was already empty and no fill ran.</returns>
    private static long AndNotBitmapWithTreeScan(
        ITermsProvider provider,
        LowLevelTransaction llt,
        ref RoaringBitmap bitmap,
        ref RoaringBitmap tempBitmap,
        CancellationToken token)
    {
        if (bitmap.IsEmpty)
            return -1;
        tempBitmap.Clear();
        long tally = FillBitmapFromTreeScan(provider, llt, ref tempBitmap, token).Tally;
        if (tempBitmap.IsEmpty)
            return tally; // subtracting nothing is a no-op
        bitmap.AndNotWith(ref tempBitmap);
        return tally;
    }
}
