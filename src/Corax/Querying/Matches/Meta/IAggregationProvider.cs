using System;
using System.Collections.Generic;

namespace Corax.Querying.Matches.Meta;

public interface IAggregationProvider
{
    public IDisposable AggregateByTerms(out List<string> terms, out Span<long> counts);
    public long AggregateByRange();

    /// <summary>
    /// Header-only scan over up to <paramref name="maxTerms"/> in-range terms (0 = all), returning the posting-count
    /// breakdown without decoding any posting ids. The two-ended range-cardinality probe samples the bottom and top
    /// of the range with this and extrapolates the unscanned middle.
    /// </summary>
    public RangePostingStats CountPostingsInRange(int maxTerms);

    /// <summary>
    /// Sub-linear estimate of how many distinct terms fall in this provider's range. Returns -1 when the range cannot
    /// be estimated cheaply (e.g. an open-ended bound), signalling the caller to fall back to a coarser bound.
    /// </summary>
    public long EstimateTermCountInRange();

    /// <summary>Total number of terms stored for the field (O(1)); feeds the combiner's global-average whale guard.</summary>
    public long TotalTermCount();
}

/// <summary>
/// Breakdown of a header-only scan over a set of in-range terms (see <see cref="IAggregationProvider.CountPostingsInRange"/>).
/// <see cref="Postings"/> is the summed posting count (which overcounts documents appearing under several terms /
/// multi-valued fields); the single / small / large split plus their sub-totals let a probe characterise the per-term
/// posting distribution without paying to decode any posting ids.
/// </summary>
public struct RangePostingStats
{
    public long Postings;        // total postings across the scanned terms (multi-valued overcount)
    public int Terms;            // number of terms scanned
    public int Singles;          // terms whose posting list is a single id (counts as 1)
    public int Smalls;           // small (inline varint) posting lists scanned
    public long SmallPostings;   // total postings across the small posting lists
    public int Larges;           // large (B+tree) posting lists scanned
    public long LargePostings;   // total postings across the large posting lists
}
