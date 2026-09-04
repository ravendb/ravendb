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
        FillBitmapFromPostingSource(postingListId, ctx.Searcher, ctx.Llt, ref ctx.Bitmaps[bitmapSlot], ctx.Token, ctx.OpLimit);
    }

    // Use as the complement for negation (NOT x means All Entries AND NOT x) 
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void CtxFillAllEntries(Matches.CompiledQueryMatch ctx, int bitmapSlot)
    {
        ctx.Bitmaps[bitmapSlot].Clear();
        OrWithMatch(ctx.Searcher.AllEntries(), ref ctx.Bitmaps[bitmapSlot], ctx.OpLimit);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void CtxFillFromTreeScan(Matches.CompiledQueryMatch ctx, int paramIndex, int bitmapSlot)
    {
        ctx.Bitmaps[bitmapSlot].Clear();
        long tally = FillBitmapFromTreeScan(ResolveTermsProvider(ref ctx.Leaves[paramIndex], ctx.Searcher, ctx.Exec), ctx.Llt, ref ctx.Bitmaps[bitmapSlot], ctx.Token, ctx.OpLimit);
        if (ctx.OpLimit != long.MaxValue) 
            return; // we cannot observe the tree scan if we didn't complete it (stopped because of the set limit). 
        ObserveTreeScanTally(ref ctx.Leaves[paramIndex], tally);
    }

    // Feed a tree-scan fill's actual count into EWMA range calibration, so next queries we'll be smarter
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void ObserveTreeScanTally(ref Planning.LeafResolveInfo leaf, long tally)
    {
        if (tally >= 0 && leaf.RangeCalibration != null)
            leaf.RangeCalibration.Observe(tally, leaf.RangeEstimate);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void CtxOrFillFromPostingSource(Matches.CompiledQueryMatch ctx, int paramIndex, int bitmapSlot)
    {
        long remaining = bitmapSlot == 0 ? ctx.OpLimit - ctx.Bitmaps[0].ComputeCount() : ctx.OpLimit;
        if (remaining <= 0) return;
        long postingListId = ResolvePostingListId(ref ctx.Leaves[paramIndex], ctx.Searcher, ctx.Exec);
        FillBitmapFromPostingSource(postingListId, ctx.Searcher, ctx.Llt, ref ctx.Bitmaps[bitmapSlot], ctx.Token, remaining);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void CtxOrFillFromTreeScan(Matches.CompiledQueryMatch ctx, int paramIndex, int bitmapSlot)
    {
        long remaining = bitmapSlot == 0 ? ctx.OpLimit - ctx.Bitmaps[0].ComputeCount() : ctx.OpLimit;
        if (remaining <= 0) return;
        FillBitmapFromTreeScan(ResolveTermsProvider(ref ctx.Leaves[paramIndex], ctx.Searcher, ctx.Exec), ctx.Llt, ref ctx.Bitmaps[bitmapSlot], ctx.Token, remaining);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void CtxOrWithMatchSlot(Matches.CompiledQueryMatch ctx, int paramIndex, int bitmapSlot)
    {
        long remaining = bitmapSlot == 0 ? ctx.OpLimit - ctx.Bitmaps[0].ComputeCount() : ctx.OpLimit;
        if (remaining <= 0) return;
        OrWithMatch(ctx.ResolvedMatches[paramIndex], ref ctx.Bitmaps[bitmapSlot], remaining, ctx.Token, ctx.PreserveLeavesForScoring);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void CtxFillFromMatch(Matches.CompiledQueryMatch ctx, int paramIndex, int bitmapSlot)
    {
        ctx.Bitmaps[bitmapSlot].Clear();
        OrWithMatch(ctx.ResolvedMatches[paramIndex], ref ctx.Bitmaps[bitmapSlot], ctx.OpLimit, ctx.Token, ctx.PreserveLeavesForScoring);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void CtxAndFromPostingSource(Matches.CompiledQueryMatch ctx, int paramIndex, int bitmapSlot)
    {
        Debug.Assert(bitmapSlot != EphemeralBitmapSlot, "AND destination must not alias the AND scratch slot.");
        long postingListId = ResolvePostingListId(ref ctx.Leaves[paramIndex], ctx.Searcher, ctx.Exec);
        AndWithPostingSource(postingListId, ctx.Searcher, ctx.Llt, ref ctx.Bitmaps[bitmapSlot], ref ctx.Bitmaps[EphemeralBitmapSlot], ctx.Token, ctx.OpLimit);
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

    [SkipLocalsInit]
    private static void FillFromPostings(ref PostingList.Iterator iterator, ref RoaringBitmap bitmap, CancellationToken token, long limit = long.MaxValue)
    {
        Span<long> buffer = stackalloc long[PostingScanBufferSize];

        long total = 0;
        while (iterator.Fill(buffer, out int read) && read > 0)
        {
            token.ThrowIfCancellationRequested();
            long remaining = limit - total;
            read = (int)Math.Min(read, remaining);
            if (read <= 0)
                break;
            EntryIdEncodings.DecodeAndDiscardFrequency(buffer, read);
            bitmap.AddRange(buffer[..read]);
            total += read;
        }
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
    /// Limit-aware AND for unordered "limit N" queries, where any N valid survivors suffice.
    /// </summary>
    [SkipLocalsInit]
    private static void AndWithPostingsLimited(ref PostingList.Iterator iterator, ref RoaringBitmap bitmap, ref RoaringBitmap tempBitmap, CancellationToken token, long limit)
    {
        if (TrySetupPostingScan(ref iterator, ref bitmap, out long pruneAfter) is false)
            return;

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
            bitmap.RemoveContainersFrom(processedKey);
            return;
        }

        bitmap.AndWithRange(ref tempBitmap, processedKey, int.MaxValue); // term complete: finish the tail
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
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    [SkipLocalsInit]
    public static void OrWithMatch(IQueryMatch match, ref RoaringBitmap bitmap, long limit = long.MaxValue, CancellationToken token = default, bool preserveLeaf = false)
    {
        if (match is IBitmapQueryMatch bm)
        {
            // Limit is intentionally ignored here: the source bitmap is already fully
            // materialized, so there's no I/O to save. Truncating would break Count
            // (used for TotalResults) and Contains (used by sorting).
            ref RoaringBitmap srcData = ref bm.BitmapState;
            if (srcData.IsEmpty)
                return;
            if (preserveLeaf)
            {
                // Score-sorted query: OrWith would steal this leaf's unique containers and mark it consumed,
                // but the score pass re-reads it afterwards. Fold a clone instead so the leaf stays intact.
                var clone = srcData.Clone();
                bitmap.OrWith(ref clone);
                clone.Dispose(); // OrWith stole only unique containers (detached from the clone); the clone still owns any shared ones, so dispose to release them.
                return;
            }
            bitmap.OrWith(ref srcData);
            return;
        }
        // A term whose relevance is stored must go through Fill: that is where TermMatch hands ids and frequencies
        // to Bm25Relevance. Bigger posting lists are re-read at score time, so they keep the fast path.
        if (match is Matches.TermMatch tm && tm.ScoringNeedsFill == false && tm.TryGetPostingListIterator(out var iter))
        {
            FillFromPostings(ref iter, ref bitmap, token, limit);
            return;
        }
        Span<long> buffer = stackalloc long[FillBufferSize];
        int read;
        long total = 0;
        while ((read = match.Fill(buffer)) > 0)
        {
            token.ThrowIfCancellationRequested();
            long remaining = limit - total;
            read = (int)Math.Min(read, remaining);
            if (read <= 0) break;
            bitmap.AddRange(buffer.Slice(0, read));
            total += read;
        }
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
    [SkipLocalsInit]
    private static void FillBitmapFromPostingSource(
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
                return;

            case TermIdMask.SmallPostingList:
                AddSmallPostingListToBitmap(llt, (long)EntryIdEncodings.GetContainerId(postingListId), ref bitmap, token, limit);
                return;

            case TermIdMask.PostingList:
                var iterator = searcher.GetPostingList(postingListId).Iterate();
                FillFromPostings(ref iterator, ref bitmap, token, limit);
                return;

            case  TermIdMask.Reserved when postingListId == EmptyPostingsId:
                return; // nothing to do
                
            default:
                throw new ArgumentOutOfRangeException(nameof(postingListId), "All posting source is not expected on a fill path.");
        }
    }

    /// <summary>AND the bitmap with a posting-list source. Bounded range scan when the source is a large
    /// PostingList; per-key membership / temp-bitmap-fill for the smaller cases. A synthetic id is either
    /// Empty (clears the bitmap — intersection with nothing) or All (universal pass-through — no-op).</summary>
    [SkipLocalsInit]
    private static void AndWithPostingSource(
        long postingListId,
        IndexSearcher searcher,
        LowLevelTransaction llt,
        ref RoaringBitmap bitmap,
        ref RoaringBitmap tempBitmap,
        CancellationToken token,
        long limit = long.MaxValue)
    {
        if (bitmap.IsEmpty)
            return;

        switch ((TermIdMask)postingListId & TermIdMask.EnsureIsSingleMask)
        {
            case TermIdMask.Single:
            {
                long entryId = (long)EntryIdEncodings.GetContainerId(postingListId);
                bool keep = bitmap.Contains(entryId);
                bitmap.Clear();
                if (keep)
                    bitmap.Add(entryId);
                return;
            }

            case TermIdMask.SmallPostingList:
                // not limiting the small posting list, since we want to limit the AND result, not the source data 
                MaterializeTermSourceIntoBitmap(postingListId, llt, ref tempBitmap, token);
                bitmap.AndWith(ref tempBitmap);
                return;

            case TermIdMask.PostingList:
                var iterator = searcher.GetPostingList(postingListId).Iterate();
                AndWithPostingsLimited(ref iterator, ref bitmap, ref tempBitmap, token, limit);
                return;

            case TermIdMask.Reserved when postingListId == AllPostingsId:
                return;
            case TermIdMask.Reserved when postingListId == EmptyPostingsId:
                bitmap.Clear();
                return;
        }
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
    private static unsafe void AddSmallPostingListToBitmap(
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
        while (total < limit && (read = reader.Fill(buffer, FillBufferSize)) > 0)
        {
            token.ThrowIfCancellationRequested();
            long remaining = limit - total;
            read = (int)Math.Min(read, remaining);
            if (read <= 0) break;
            var results = new Span<long>(buffer, read);
            EntryIdEncodings.DecodeAndDiscardFrequency(results, read);
            bitmap.AddRange(results[..read]);
            total += read;
        }
    }

    /// <summary>
    /// Fill a bitmap by walking an ITermsProvider's posting list IDs in batches.
    /// Each batch is partitioned into three buckets keyed by TermIdMask:
    ///   - Single: container ID strip + sort/dedup, then bitmap.AddRange.
    ///   - SmallPostingList: container ID strip + sort/dedup, batch Container.GetAll,
    ///     decode each posting list inline via FastPForBufferedReader.
    ///   - PostingList: container ID strip + sort/dedup, then iterate each via FillFromPostings.
    /// Partitioning is branchless: (id &amp; EnsureIsSingleMask) yields the bucket index.
    /// </summary>
    /// <returns>The over-counting postings tally: the running sum of posting-list sizes fed into the
    /// bitmap (singles + small + large bucket <c>PostingListState.NumberOfEntries</c>), counting
    /// multi-valued documents once per matching term. This is exactly the quantity
    /// <c>EstimateMatchesInRange</c> predicts, so callers can Observe(tally, estimate) to calibrate it —
    /// but only when the fill was unbounded (<paramref name="limit"/> == long.MaxValue); a truncated fill
    /// stops early and the tally is a partial undercount.</returns>
    [SkipLocalsInit]
    public static unsafe long FillBitmapFromTreeScan(
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
        // While upperBound < limit the real count cannot have reached the limit, so expensive bitmap.ComputeCount() can be skipped.
        // Limit is checked on a per batch boundary, not exact, caller needs to check it
        long upperBound = 0;
        try
        {
            int read;
            while ((upperBound < limit || bitmap.ComputeCount() < limit) && (read = provider.FillPostingListIds(plIds)) > 0)
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
                        for (int i = 0; i < smallLen && (upperBound < limit || bitmap.ComputeCount() < limit); i++)
                        {
                            var item = containerItems[i];
                            _ = VariableSizeEncoding.Read<int>(item.Address, out var offset);
                            smallListReader.Init(item.Address + offset, item.Length - offset);

                            int smallRead;
                            while ((upperBound < limit || bitmap.ComputeCount() < limit) && (smallRead = smallListReader.Fill(pEntryBuffer, entryBuffer.Length)) > 0)
                            {
                                token.ThrowIfCancellationRequested();
                                EntryIdEncodings.DecodeAndDiscardFrequency(entryBuffer, smallRead);
                                bitmap.AddRange(entryBuffer[..smallRead]);
                                upperBound += smallRead;
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
                    for (int i = 0; i < largeLen && (upperBound < limit || bitmap.ComputeCount() < limit); i++)
                    {
                        var setStateSpan = Container.GetReadOnly(llt, new ContainerEntryId(largeSpan[i]));
                        ref readonly var setState = ref MemoryMarshal.AsRef<PostingListState>(setStateSpan);
                        using var postingList = new PostingList(llt, Slices.Empty, in setState);
                        var iterator = postingList.Iterate();
                        FillFromPostings(ref iterator, ref bitmap, token);

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

        return upperBound;
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
        long tally = FillBitmapFromTreeScan(provider, llt, ref tempBitmap, token);
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
        long tally = FillBitmapFromTreeScan(provider, llt, ref tempBitmap, token);
        if (tempBitmap.IsEmpty)
            return tally; // subtracting nothing is a no-op
        bitmap.AndNotWith(ref tempBitmap);
        return tally;
    }
}
