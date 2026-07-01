using System;
using System.Collections.Generic;
using System.Diagnostics;
using Corax.Querying.Matches.Meta;

namespace Corax.Querying.Matches;

/// <summary>
/// Chains an inner IQueryMatch through one or more additional filter matches.
/// On Fill(), retrieves entries from the inner match, then applies each
/// filter's AndWith() to narrow the results. Currently only used to apply
/// spatial predicates after the bitmap filter phase builds the candidate set,
/// but the construct itself is just an AndWith-chain — nothing here is
/// spatial-specific.
///
/// When timing capture is enabled, records per-call wall time and survivor counts for the inner Fill
/// and each post-filter AndWith; when disabled, those writes are skipped.
/// </summary>
public sealed class PostFilterMatch : IQueryMatch
{
    private readonly IQueryMatch _inner;
    private readonly IQueryMatch[] _postFilters;

    // True when introspection timing capture was requested. Gates all counter writes.
    private readonly bool _wantTimings;

    // Wall-clock ticks (Stopwatch.GetTimestamp()) spent inside the inner Fill / AndWith.
    private long _innerTicks;
    // Cumulative number of entries the inner returned across all calls.
    private long _innerEmitted;

    // Per-post-filter cumulative metrics; null when timing capture is disabled.
    private readonly long[] _filterTicks;       // ticks inside that filter's AndWith
    private readonly long[] _filterSurvivors;   // survivors after that filter ran
    private readonly long[] _filterRejected;    // rejections (input - survivors)

    public PostFilterMatch(IQueryMatch inner, IQueryMatch[] postFilters, bool wantTimings)
    {
        _inner = inner;
        _postFilters = postFilters;
        _wantTimings = wantTimings;
        if (wantTimings)
        {
            _filterTicks = new long[postFilters.Length];
            _filterSurvivors = new long[postFilters.Length];
            _filterRejected = new long[postFilters.Length];
        }
    }

    public long Count => _inner.Count;
    public bool IsBoosting => _inner.IsBoosting;

    public int Fill(Span<long> matches)
    {
        long t0 = _wantTimings ? Stopwatch.GetTimestamp() : 0;
        int read = _inner.Fill(matches);
        if (_wantTimings)
        {
            _innerTicks += Stopwatch.GetTimestamp() - t0;
            _innerEmitted += read;
        }

        return read == 0 ? 0 : ApplyPostFilters(matches, read);
    }

    private int ApplyPostFilters(Span<long> buffer, int count)
    {
        for (int i = 0; i < _postFilters.Length; i++)
        {
            int input = count;
            long ti = _wantTimings ? Stopwatch.GetTimestamp() : 0;
            count = FilterSpan(_postFilters[i], buffer, count);
            if (_wantTimings)
            {
                _filterTicks[i] += Stopwatch.GetTimestamp() - ti;
                _filterSurvivors[i] += count;
                _filterRejected[i] += input - count;
            }

            if (count == 0)
                return 0;
        }

        return count;
    }

    private static int FilterSpan(IQueryMatch filter, Span<long> buffer, int count)
    {
        return filter switch
        {
            IPostFilterMatch postFilter => postFilter.AndWith(buffer, count),
            IBitmapQueryMatch bitmapMatch => bitmapMatch.BitmapState.AndWith(buffer, count),
            EmptyQueryMatch => 0, 
            _ => throw new InvalidOperationException($"Unexpected post-filter match type {filter.GetType().Name}; only spatial post-filters (per-entry, bitmap, or empty) are expected.")
        };
    }

    public void Score(Span<long> matches, Span<float> scores, float boostFactor)
    {
        _inner.Score(matches, scores, boostFactor);
        for (int i = 0; i < _postFilters.Length; i++)
            _postFilters[i].Score(matches, scores, boostFactor);
    }

    public void ScoreSorted(Span<long> matches, Span<float> scores, float boostFactor)
    {
        _inner.ScoreSorted(matches, scores, boostFactor);
        for (int i = 0; i < _postFilters.Length; i++)
            _postFilters[i].ScoreSorted(matches, scores, boostFactor);
    }

    public QueryInspectionNode Inspect()
    {
        var parameters = new Dictionary<string, string>();

        if (_wantTimings)
        {
            double tickFreq = Stopwatch.Frequency / 1000.0;
            if (_innerTicks > 0)
                parameters["Inner_ms"] = (_innerTicks / tickFreq).ToString("F3");
            parameters["InnerEmitted"] = _innerEmitted.ToString();

            long matched = _postFilters.Length == 0 ? _innerEmitted : _filterSurvivors[^1];
            parameters[Constants.QueryInspectionNode.MatchedResults] = matched.ToString("N0");

            for (int i = 0; i < _postFilters.Length; i++)
            {
                string prefix = $"Filter[{i}]_";
                if (_filterTicks[i] > 0)
                    parameters[prefix + "ms"] = (_filterTicks[i] / tickFreq).ToString("F3");
                parameters[prefix + "kept"] = _filterSurvivors[i].ToString();
                parameters[prefix + "rejected"] = _filterRejected[i].ToString();
            }
        }

        var children = new List<QueryInspectionNode>(_postFilters.Length + 1)
        {
            _inner.Inspect()
        };
        for (int i = 0; i < _postFilters.Length; i++)
            children.Add(_postFilters[i].Inspect());

        return new QueryInspectionNode(nameof(PostFilterMatch), parameters: parameters, children: children);
    }
}
