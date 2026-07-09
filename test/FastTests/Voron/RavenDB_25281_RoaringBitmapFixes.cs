using Corax.Querying.Matches;
using Sparrow.Server;
using Sparrow.Threading;
using Tests.Infrastructure;
using Voron.Data.RoaringBitmaps;
using Xunit;

namespace FastTests.Voron;

// RavenDB-25281: four latent correctness bugs in RoaringBitmap's set operations / cardinality
// tracking, and in BitmapMatch.Count, each fixed by mirroring an existing correct sibling in the
// same file. See scratchpad checkup/V8-A-roaring.md for the full verification writeup.
public unsafe class RavenDB_25281_RoaringBitmapFixes : NoDisposalNeeded
{
    public RavenDB_25281_RoaringBitmapFixes(ITestOutputHelper output) : base(output)
    {
    }

    // RB1: ComputeCount() must not double-count values that ended up duplicated inside an
    // ArrayUnsorted container (e.g. via two OrWith merges landing the same value in one container),
    // because ComputeCount() never runs PrepareForReading()/dedup. Force the ArrayUnsorted shape by
    // Add()-ing a Range-container value followed by a non-adjacent value in the same container key,
    // which ConvertRangeToArray marks ArrayUnsorted (data is appended, not verified sorted).
    [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Voron)]
    public void ComputeCount_DoesNotInflate_OnUnsortedDuplicates()
    {
        using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);
        var a = new RoaringBitmap(ctx);
        var b = new RoaringBitmap(ctx);
        try
        {
            a.Add(5);
            a.Add(5000); // container 0 becomes ArrayUnsorted: {5, 5000}

            b.Add(5);
            b.Add(6000); // container 0 becomes ArrayUnsorted: {5, 6000}

            a.OrWith(ref b); // LazyOrWith concatenates array containers -> ArrayUnsorted {5, 5000, 5, 6000}

            const long distinctCount = 3; // {5, 5000, 6000}
            Assert.Equal(distinctCount, a.ComputeCount());

            // Cross-check: after an explicit repair, ComputeCount must still agree (proves the fix
            // isn't just moving the dedup to a different reported number).
            a.PrepareForReading();
            Assert.Equal(distinctCount, a.ComputeCount());
        }
        finally
        {
            a.Dispose();
            b.Dispose();
        }
    }

    // RB2: AndNotWith must resolve a possibly-lazy Bitmap cardinality (like AndWith already does
    // via ResolveCardinality) before deciding whether to free the container. Otherwise a container
    // that ANDNOT zeroed out entirely keeps its stale Cardinality == LazyCardinality (-1), so the
    // `== 0` check never frees it, leaving _containerCount > 0 and IsEmpty wrongly false.
    [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Voron)]
    public void AndNotWith_FullySubtractedBitmapContainer_IsEmptyIsTrue()
    {
        using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);
        var a = new RoaringBitmap(ctx);
        var b = new RoaringBitmap(ctx);
        try
        {
            // Force container 0 into a Bitmap container: sequential Add stays a cheap Range
            // container forever (TryMergeRangeInPlace always merges the next contiguous value), so
            // add every-other value (non-contiguous) up to > ArrayContainerMaxCardinality (4096)
            // distinct entries, which promotes Array/ArrayUnsorted -> Bitmap once full.
            for (int i = 0; i < 10000; i += 2)
                a.Add(i);
            for (int i = 0; i < 10000; i += 2)
                b.Add(i); // b covers exactly a's range

            a.AndNotWith(ref b); // a should now be fully empty

            Assert.True(a.IsEmpty); // fails today: _containerCount stays 1, Cardinality left at LazyCardinality (-1)
            Assert.Equal(-1, a.MinContainerKey);
            Assert.Equal(-1, a.MaxContainerKey);
            Assert.Equal(0, a.ComputeCount());
        }
        finally
        {
            a.Dispose();
            b.Dispose();
        }
    }

    // RB3: AndContainerInPlace takes rightType BY VALUE, so when it internally resolves an
    // ArrayUnsorted right-side container via SortAndDedupSmallArray (updating the local copy of
    // rightType to Array), that corrected label is never written back to other._types. AndWithRange
    // is the one caller that doesn't consume `other` afterward (unlike AndWith/AndNotWith, which
    // MarkConsumed it), so the stale ArrayUnsorted label is observable directly off `other`.
    // Cardinality must exceed SimdLinearScanThreshold (64) on at least one side so the merge takes
    // the galloping-merge branch (which calls SortAndDedupSmallArray) rather than the SIMD
    // cross-AND branch (which tolerates unsorted input and never touches rightType at all).
    [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Voron)]
    public void AndWithRange_LeavesCorrectTypeLabel_OnRightSideArrayUnsorted()
    {
        using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);
        var left = new RoaringBitmap(ctx);
        var right = new RoaringBitmap(ctx);
        try
        {
            // Build container 0 on both sides as ArrayUnsorted with cardinality > SimdLinearScanThreshold
            // (64), so the AND takes the galloping-merge branch (which calls SortAndDedupSmallArray on
            // an ArrayUnsorted side) rather than the SIMD cross-AND branch (small arrays, tolerates
            // unsorted input, never touches rightType). Two non-adjacent values first break out of the
            // cheap contiguous Range container (ConvertRangeForAdd marks it ArrayUnsorted); subsequent
            // descending appends keep it unsorted while growing well past the SIMD threshold.
            right.Add(0);
            right.Add(200);
            for (int v = 199; v >= 100; v--)
                right.Add(v); // container 0 on right: ArrayUnsorted, cardinality 102 > SimdLinearScanThreshold

            left.Add(0);
            left.Add(200);
            for (int v = 199; v >= 100; v--)
                left.Add(v); // container 0 on left: ArrayUnsorted, cardinality 102 > SimdLinearScanThreshold, overlaps right entirely

            left.AndWithRange(ref right, 0, 1);

            // White-box: RoaringBitmap grants InternalsVisibleTo to FastTests, so _types/_index are
            // directly readable here. AndContainerInPlace's Array/ArrayUnsorted x Array/ArrayUnsorted
            // branch sorts+dedups an ArrayUnsorted right side in place (the data mutation is real and
            // correct); the label written back to other._types must reflect that, i.e. Array, not the
            // stale ArrayUnsorted.
            int slot = right._index[0];
            Assert.True(slot >= 0);
            Assert.Equal(ContainerType.Array, right._types[slot]);
        }
        finally
        {
            left.Dispose();
            right.Dispose();
        }
    }

    // RB4: BitmapMatch.Count must resolve the bitmap (PrepareForReading, same as Fill already does)
    // before computing the count, mirroring every other IBitmapQueryMatch implementor
    // (CompiledQueryMatch, LazyOrMatch). Otherwise Count reads ComputeCount() directly and inherits
    // RB1's ArrayUnsorted-duplicate overcount whenever Count/Inspect is read before the first Fill.
    [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Voron)]
    public void BitmapMatch_Count_MatchesFillCount_BeforeFillIsCalled()
    {
        using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);
        var match = new BitmapMatch(ctx);
        var other = new RoaringBitmap(ctx);
        try
        {
            ref RoaringBitmap bmp = ref match.BitmapState;

            bmp.Add(5);
            bmp.Add(5000); // container 0 on bmp: ArrayUnsorted {5, 5000}

            other.Add(5);
            other.Add(6000); // container 0 on other: ArrayUnsorted {5, 6000}

            bmp.OrWith(ref other); // duplicate '5' now sits twice in bmp's ArrayUnsorted container 0

            long countBeforeFill = match.Count; // reads ComputeCount() with no PrepareForReading

            System.Span<long> buf = stackalloc long[16];
            int total = 0, read;
            while ((read = match.Fill(buf)) > 0)
                total += read;

            Assert.Equal(total, countBeforeFill); // fails today: countBeforeFill > total (off by the duplicate)
        }
        finally
        {
            match.Dispose();
        }
    }
}
