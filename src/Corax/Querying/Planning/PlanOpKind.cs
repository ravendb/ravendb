namespace Corax.Querying.Planning;

/// <summary>
/// The low level operations that makes up an executed query
/// </summary>
public enum PlanOpKind : byte
{
    /// <summary>Seed bitmap from a native posting-list leaf.</summary>
    FillFromPostingSource,

    /// <summary>Seed bitmap from a CompactTree-scan leaf.</summary>
    FillFromTreeScan,

    /// <summary>Seed bitmap from an IQueryMatch leaf (spatial / vector / search / boosted, and the match-all plan).</summary>
    FillFromMatch,

    /// <summary>Seed <c>bitmap[BitmapLocal]</c> with every entry via <c>Searcher.AllEntries()</c> — used for match all / negation with complement.</summary>
    FillAllEntries,

    /// <summary>Intersect bitmap with a posting-list leaf; stop the plan if the result is empty, unless <see cref="PlanOp.SkipEarlyExit"/> is set.</summary>
    AndFromPostingSource,

    /// <summary>Intersect bitmap with a tree-scan leaf. <see cref="AndFromPostingSource"/> semantics.</summary>
    AndFromTreeScan,

    /// <summary>Intersect bitmap with an IQueryMatch leaf. <see cref="AndFromPostingSource"/> semantics.</summary>
    AndFromMatch,

    /// <summary>Union a posting-list leaf into a bitmap. When the target is slot 0, stop once the page limit is reached.</summary>
    OrFromPostingSource,

    /// <summary>Union a tree-scan leaf into a bitmap. <see cref="OrFromPostingSource"/> semantics.</summary>
    OrFromTreeScan,

    /// <summary>Union an IQueryMatch leaf into a bitmap. <see cref="OrFromPostingSource"/> semantics.</summary>
    OrFromMatch,

    /// <summary>Subtract a posting-list leaf from bitmap.</summary>
    AndNotFromPostingSource,

    /// <summary>Subtract a tree-scan leaf from bitmap. <see cref="AndNotFromPostingSource"/> semantics.</summary>
    AndNotFromTreeScan,

    /// <summary>Subtract an IQueryMatch leaf from bitmap. <see cref="AndNotFromPostingSource"/> semantics.</summary>
    AndNotFromMatch,

    /// <summary>Union a contiguous run of posting-list leaves (an expanded IN) into bitmap[BitmapLocal].
    /// ParamIndex2 = index into ctx.InRangeCounts for the runtime count.</summary>
    InRangeFromPostingSource,

    /// <summary>Union a contiguous run of IQueryMatch leaves (a boosted IN) into bitmap[BitmapLocal].</summary>
    InRangeFromMatch,

    /// <summary>Intersect a contiguous run of posting-list leaves (an AllIn) with bitmap,
    /// stopping early on an empty result unless <see cref="PlanOp.SkipEarlyExit"/> is set.</summary>
    AllInRangeFromPostingSource,

    /// <summary>Intersect a contiguous run of IQueryMatch leaves (a boosted AllIn) with bitmap.</summary>
    AllInRangeFromMatch,

    ClearBitmap,

    /// <summary>Intersect two bitmap slots. BitmapLocal = target, ParamIndex2 = source.</summary>
    AndBitmaps,

    /// <summary>Subtract the source slot from the target slot. BitmapLocal = target, ParamIndex2 = source.</summary>
    AndNotBitmaps,

    /// <summary>Lazy-union two bitmap slots — defers container merging for speed, so the result
    /// bitmap is repaired once at the done label before it can be iterated. BitmapLocal = target, ParamIndex2 = source.</summary>
    LazyOrBitmaps,

    /// <summary>Short-circuit the plan when a bitmap slot is empty.</summary>
    GotoDoneIfEmpty,

    /// <summary>Switch to the entry-scan tail when bitmap is small relative to the next clause's
    /// cardinality (cheaper to scan the surviving entries than to keep intersecting).</summary>
    MaybeEntryScan,

    /// <summary>Terminal op: jump to the done label that ends the bitmap pipeline.</summary>
    GotoDone,
}
