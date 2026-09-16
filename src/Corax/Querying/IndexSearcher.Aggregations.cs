using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;
using Corax.Mappings;
using Corax.Querying.Matches;
using Corax.Querying.Matches.Meta;
using Corax.Querying.Matches.TermsProviders;
using Corax.Querying.Planning;
using Voron;
using Voron.Data.CompactTrees;
using Voron.Data.Lookups;
using Range = Corax.Querying.Matches.Meta.Range;

namespace Corax.Querying;

public partial class IndexSearcher
{
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public IAggregationProvider TextualAggregation(in FieldMetadata field, bool forward = true, in CancellationToken token = default)
    {
        var compactTree = GetTermsFor(field.FieldName);
        if (compactTree is null)
            return new EmptyAggregationProvider();
        
        return forward
            ? new ExistsTermsProvider<Lookup<CompactTree.CompactKeyLookup>.ForwardIterator>(this, compactTree, field)
            : new ExistsTermsProvider<Lookup<CompactTree.CompactKeyLookup>.BackwardIterator>(this, compactTree, field);
    }

    public IAggregationProvider LowAggregationBuilder<TValue>(in FieldMetadata field, TValue value, ComparisonOperator operation, bool forward)
    {
        Debug.Assert(value is double or string, "value is double or string");
        Debug.Assert(operation is ComparisonOperator.LessThan or ComparisonOperator.LessThanOrEqual);
        
        return value switch
        {
            double d => BetweenAggregation(field, double.MinValue, d, ComparisonOperator.GreaterThanOrEqual, rightSide: operation,
                forward),
            string s => BetweenAggregation(field, Slices.BeforeAllKeys, EncodeAndApplyAnalyzer(default, s), ComparisonOperator.GreaterThanOrEqual,
                operation, forward),
            _ => throw new ArgumentOutOfRangeException(nameof(value), value, null)
        };
    }

    public IAggregationProvider GreaterAggregationBuilder<TValue>(in FieldMetadata field, TValue value, ComparisonOperator operation, bool forward)
    {
        Debug.Assert(operation is ComparisonOperator.GreaterThan or ComparisonOperator.GreaterThanOrEqual);
        Debug.Assert(value is double or string, "value is double or string");
        
        return value switch
        {
            double d => BetweenAggregation(field, d, double.MaxValue, operation, rightSide: ComparisonOperator.LessThanOrEqual,
                forward),
            string s => BetweenAggregation(field, EncodeAndApplyAnalyzer(default, s), Slices.AfterAllKeys, operation,
                ComparisonOperator.LessThanOrEqual, forward),
            _ => throw new ArgumentOutOfRangeException(nameof(value), value, null)
        };
    }
    
    public IAggregationProvider BetweenAggregation<TValue>(in FieldMetadata field, TValue low, TValue high,
        ComparisonOperator leftSide = ComparisonOperator.GreaterThanOrEqual, ComparisonOperator rightSide = ComparisonOperator.LessThanOrEqual, bool forward = true)
    {
        Debug.Assert(low is double or long or string or Slice, "value is double, long, string or Slice");

        // Map the (low, high) inclusivity pair to compile-time range markers; the value-type fan-out
        // (double / long / string / Slice) lives in the generic builder this dispatches to.
        return (leftSide, rightSide) switch
        {
            // (x, y)
            (ComparisonOperator.GreaterThan, ComparisonOperator.LessThan) =>
                AggregationRangeBuilder<TValue, Range.Exclusive, Range.Exclusive>(field, low, high, forward),
            //<x, y)
            (ComparisonOperator.GreaterThanOrEqual, ComparisonOperator.LessThan) =>
                AggregationRangeBuilder<TValue, Range.Inclusive, Range.Exclusive>(field, low, high, forward),
            //<x, y>
            (ComparisonOperator.GreaterThanOrEqual, ComparisonOperator.LessThanOrEqual) =>
                AggregationRangeBuilder<TValue, Range.Inclusive, Range.Inclusive>(field, low, high, forward),
            //(x, y>
            (ComparisonOperator.GreaterThan, ComparisonOperator.LessThanOrEqual) =>
                AggregationRangeBuilder<TValue, Range.Exclusive, Range.Inclusive>(field, low, high, forward),
            _ => throw new ArgumentOutOfRangeException($"Unknown operation at {nameof(BetweenQuery)}.")
        };
    }

    // Two-ended probe + combiner. Cheaply estimates how many *documents* match a range without scanning it: it samples
    // the posting-count distribution at the bottom and top of the range, gets a sub-linear estimate of the in-range
    // term count, and extrapolates the unscanned middle assuming a similar per-term density. Open bounds are estimated
    // directly (the term-count descent walks to the edge of the tree), so every range yields a concrete, non-negative
    // estimate capped at NumberOfEntries.
    private const int RangeBottomSample = 512;
    private const int RangeTopSample = 256;

    // Clamp on the per-clause calibration multiplier applied to the range estimate (see EstimateMatchesInRange).
    // Keeps a noisy or pathological single run from blowing the estimate up or collapsing it: at most 4x up or
    // 1/4 down per clause.
    private const double CalibrationMultiplierMin = 0.25;
    private const double CalibrationMultiplierMax = 4.0;

    public long EstimateMatchesInRange<TValue>(in FieldMetadata field, TValue low, TValue high,
        out RangeEstimateBreakdown breakdown,
        ComparisonOperator leftSide = ComparisonOperator.GreaterThanOrEqual,
        ComparisonOperator rightSide = ComparisonOperator.LessThanOrEqual,
        double calibrationFactor = 0)
    {
        breakdown = new RangeEstimateBreakdown { CalibrationFactor = calibrationFactor };

        var forward = BetweenAggregation(field, low, high, leftSide, rightSide, forward: true);

        long terms = forward.EstimateTermCountInRange();
        breakdown.RangeTerms = terms;
        if (terms == 0)
        {
            breakdown.IsExact = true;
            return 0;
        }

        // Scan the bottom of the range. If we never hit the cap, we have walked every in-range term: the count is exact.
        RangePostingStats bottom = forward.CountPostingsInRange(RangeBottomSample);
        if (bottom.Terms < RangeBottomSample)
        {
            long exact = Math.Min(bottom.Postings, NumberOfEntries);
            breakdown.IsExact = true;
            breakdown.SampledTerms = bottom.Terms;
            breakdown.SampledPostings = bottom.Postings;
            breakdown.RawEstimate = exact;
            breakdown.Estimate = exact;
            return exact;
        }

        // Cap the top sample so it cannot overlap the bottom sample (matters only for ranges barely above the cap).
        int topCap = (int)Math.Min(RangeTopSample, Math.Max(0, terms - bottom.Terms));
        if (topCap == 0)
        {
            long exact = Math.Min(bottom.Postings, NumberOfEntries);
            breakdown.IsExact = true;
            breakdown.SampledTerms = bottom.Terms;
            breakdown.SampledPostings = bottom.Postings;
            breakdown.RawEstimate = exact;
            breakdown.Estimate = exact;
            return exact;
        }

        var backward = BetweenAggregation(field, low, high, leftSide, rightSide, forward: false);
        RangePostingStats top = backward.CountPostingsInRange(topCap);

        long sampledTerms = bottom.Terms + top.Terms;
        long sampledPostings = bottom.Postings + top.Postings;
        long middleTerms = Math.Max(0, terms - sampledTerms);

        double sampledAvg = (double)sampledPostings / sampledTerms;

        // Field-wide density (total docs / total terms, both O(1)). This is what the unscanned middle would
        // average if it looked like the field as a whole, and is the floor a "whale" (a dense term hiding in
        // the middle, invisible to the edge samples) would push the true average toward.
        long totalTerms = forward.TotalTermCount();
        double globalAvg = totalTerms > 0 ? (double)NumberOfEntries / totalTerms : sampledAvg;

        // === Unscanned-middle extrapolation: Bayesian shrinkage toward the global density ===
        //
        // We know the edge density (sampledAvg) and the field-wide density (globalAvg) but not the middle's.
        // Naive bounds: trust sampledAvg blindly UNDER-estimates a dense range (a "whale" hidden in the
        // middle is missed); snapping to globalAvg OVER-estimates a genuinely sparse range. Instead shrink
        // the middle density toward globalAvg, strength proportional to sampled coverage:
        //
        //     middleAvg = (sampledPostings + k*globalAvg) / (sampledTerms + k)   // k pseudo-obs at globalAvg
        //
        // With beta fixed at 1, k = middleTerms and this is the coverage blend coverage*sampledAvg +
        // (1-coverage)*globalAvg, coverage = sampledTerms/(sampledTerms+middleTerms): well-sampled trusts its
        // edges, barely-sampled defers to global. It is whale-cautious (it can only raise a sparse-looking
        // estimate, never lower it). Per-clause calibration is applied separately as a direct multiplier below,
        // NOT folded in here as the shrinkage strength — that would correct the estimate only sublinearly and
        // pin a repeated, already-counted clause at a wrong ratio instead of converging.
        //
        // Worked example: 1,000,000 docs over 100,000 terms -> globalAvg = 10. Sparse range of 1500 terms,
        // sampledTerms = 768 (sampledAvg = 2, sampledPostings = 1536), middleTerms = 732:
        //     middleAvg = (1536 + 732*10)/(768+732) = 5.9 -> estimate 5855.
        const double beta = 1.0;
        double k = beta * middleTerms;
        double middleAvg = (sampledPostings + k * globalAvg) / (sampledTerms + k);

        long rawEstimate = Math.Min(sampledPostings + (long)(middleTerms * middleAvg), NumberOfEntries);

        // Apply the learned calibration as a DIRECT multiplier: the EWMA Factor is actual/RawEstimate (it observes
        // RawEstimate as "predicted"), so estimate = RawEstimate * Factor converges to the measured actual in a
        // single observation (InflationEwma seeds on the first sample). 0 = no history -> neutral 1.0. Clamped so a
        // single pathological run can't blow the estimate up or collapse it.
        double mult = calibrationFactor <= 0 ? 1.0 : Math.Clamp(calibrationFactor, CalibrationMultiplierMin, CalibrationMultiplierMax);
        long estimate = Math.Min((long)(rawEstimate * mult), NumberOfEntries);

        breakdown.SampledTerms = sampledTerms;
        breakdown.SampledPostings = sampledPostings;
        breakdown.MiddleTerms = middleTerms;
        breakdown.SampledAvg = sampledAvg;
        breakdown.GlobalAvg = globalAvg;
        breakdown.Beta = beta;
        breakdown.K = k;
        breakdown.MiddleAvg = middleAvg;
        breakdown.RawEstimate = rawEstimate;
        breakdown.Estimate = estimate;
        return estimate;
    }

    // StartsWith(prefix) is the contiguous byte-range [encodedPrefix, successor(encodedPrefix)) — the prefix is
    // analyzer-encoded like stored terms and the CompactTree sorts lexicographically, so every prefix match is one
    // block. Reuses the range estimator (TryWritePrefixSuccessor computes the upper bound) so StartsWith costs two
    // descents instead of falling back to the whole-index size.
    public long EstimateStartsWith(in FieldMetadata field, string prefix, out RangeEstimateBreakdown breakdown, double calibrationFactor = 0)
    {
        Slice encodedPrefix = EncodeAndApplyAnalyzer(field, prefix);
        ReadOnlySpan<byte> prefixBytes = encodedPrefix.AsReadOnlySpan();

        if (prefixBytes.Length > 0)
        {
            using var _ = Allocator.Allocate(prefixBytes.Length, out Span<byte> successor);
            int len = TryWritePrefixSuccessor(prefixBytes, successor);
            if (len > 0)
            {
                using var __ = Slice.From(Allocator, successor.Slice(0, len), out Slice high);
                return EstimateMatchesInRange(field, encodedPrefix, high, out breakdown,
                    ComparisonOperator.GreaterThanOrEqual, ComparisonOperator.LessThan, calibrationFactor);
            }
        }

        // empty prefix or all-0xFF carry: no finite successor, so the match set runs to the end of the tree
        return EstimateMatchesInRange(field, encodedPrefix, Slices.AfterAllKeys, out breakdown,
            ComparisonOperator.GreaterThanOrEqual, ComparisonOperator.LessThanOrEqual, calibrationFactor);
    }

    // Writes successor(prefix) — the exclusive upper bound of a StartsWith(prefix) scan — into dest and returns its
    // length. The successor drops trailing 0xFF bytes and increments the last remaining byte; every key with the
    // prefix sorts in [prefix, successor). Returns 0 when the prefix is empty or all 0xFF: no finite successor exists,
    // so the prefix's match block runs to the end of the tree (callers use AfterAllKeys / a backward Reset instead).
    // dest must be at least prefix.Length bytes. Shared by the range estimator and the backward StartsWith seek limit
    // so the two stay consistent.
    internal static int TryWritePrefixSuccessor(ReadOnlySpan<byte> prefix, Span<byte> dest)
    {
        int len = prefix.Length;
        while (len > 0 && prefix[len - 1] == 0xFF)
            len--;

        if (len == 0)
            return 0;

        prefix.Slice(0, len).CopyTo(dest);
        dest[len - 1]++;
        return len;
    }

    private IAggregationProvider AggregationRangeBuilder<TLow, THigh>(in FieldMetadata field, Slice low, Slice high, bool forward)
        where TLow : struct, Range.Marker
        where THigh : struct, Range.Marker
    {
        if (_fieldsTree == null || _fieldsTree.TryGetCompactTreeFor(field.FieldName, out var terms) == false)
            return new EmptyAggregationProvider();

        return forward switch
        {
            true => new TermsRangeProvider<Lookup<CompactTree.CompactKeyLookup>.ForwardIterator, TLow, THigh>(this, terms, field, low, high),
            false => new TermsRangeProvider<Lookup<CompactTree.CompactKeyLookup>.BackwardIterator, TLow, THigh>(this, terms, field, low, high)
        };
    }


    // Single place the runtime value type fans out to its lookup-key/term-type pair. Numeric values (double,
    // long) go through their dedicated numeric lookup; string/Slice share the textual builder (strings are
    // analyzer-encoded to a Slice first). longs are kept as longs so full precision is preserved end to end.
    private IAggregationProvider AggregationRangeBuilder<TValue, TLow, THigh>(in FieldMetadata field, TValue low, TValue high, bool forward)
        where TLow : struct, Range.Marker
        where THigh : struct, Range.Marker
    {
        if (typeof(TValue) == typeof(double))
            return AggregationRangeBuilder<DoubleLookupKey, double, TLow, THigh>(field, new((double)(object)low), new((double)(object)high), forward);

        if (typeof(TValue) == typeof(long))
            return AggregationRangeBuilder<Int64LookupKey, long, TLow, THigh>(field, new((long)(object)low), new((long)(object)high), forward);

        if (typeof(TValue) == typeof(string))
            return AggregationRangeBuilder<TLow, THigh>(field, EncodeAndApplyAnalyzer(default, (string)(object)low), EncodeAndApplyAnalyzer(default, (string)(object)high), forward);

        if (typeof(TValue) == typeof(Slice))
            return AggregationRangeBuilder<TLow, THigh>(field, (Slice)(object)low, (Slice)(object)high, forward);

        throw new ArgumentException($"{typeof(TValue)} is not supported in {nameof(BetweenQuery)}");
    }


    private IAggregationProvider AggregationRangeBuilder<TLookupKey, TTermType, TLow, THigh>(FieldMetadata field, TLookupKey low, TLookupKey high, bool forward)
        where TLow : struct, Range.Marker
        where THigh : struct, Range.Marker
        where TLookupKey : struct, ILookupKey
    {
        field = field.GetNumericFieldMetadata<TTermType>(Allocator);
        var set = _fieldsTree != null && _fieldsTree.TryGetLookupFor<TLookupKey>(field.FieldName, out var lookup) ? lookup : null;
        if (set is null || set.NumberOfEntries == 0)
            return new EmptyAggregationProvider();

        return forward switch
        {
            true => new TermsNumericRangeProvider<Lookup<TLookupKey>.ForwardIterator, TLow, THigh, TLookupKey>(this, set, field, low, high),
            false => new TermsNumericRangeProvider<Lookup<TLookupKey>.BackwardIterator, TLow, THigh, TLookupKey>(this, set, field, low, high)
        };
    }
}
