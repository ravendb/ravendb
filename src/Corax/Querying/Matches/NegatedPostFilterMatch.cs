using System;
using System.Collections.Generic;
using System.Threading;
using Corax.Querying.Matches.Meta;
using Corax.Querying.Primitives;
using Voron.Data.RoaringBitmaps;

namespace Corax.Querying.Matches;

/// <summary>
/// Applies one or more negated post-filters (`not spatial.within(...)` / `not vector.search(...)`) as a single global set-difference. 
/// </summary>
public sealed class NegatedPostFilterMatch : IQueryMatch, IDisposable
{
    private readonly IndexSearcher _searcher;
    private readonly IQueryMatch _universe;
    private readonly Func<IQueryMatch, IQueryMatch>[] _negatedFactories;
    private readonly CancellationToken _token;

    private BitmapMatch _result; 
    private RoaringBitmap _temp;
    private bool _initialized;
    private bool _disposed;

    private readonly List<IQueryMatch> _builtClauses = [];

    public NegatedPostFilterMatch(IndexSearcher searcher, IQueryMatch universe, Func<IQueryMatch, IQueryMatch>[] negatedFactories, CancellationToken token = default)
    {
        _searcher = searcher;
        _universe = universe;
        _negatedFactories = negatedFactories;
        _token = token;
    }

    public long Count => _universe.Count; // Intentional upper-bound estimate

    public bool IsBoosting => false;

    private void EnsureInitialized()
    {
        if (_initialized)
            return;
        _initialized = true;

        var allocator = _searcher.Allocator;
        _result = new BitmapMatch(allocator);
        _temp = new RoaringBitmap(allocator);

        // preserveLeaf: true — _universe.Count/Inspect() are read again after EnsureInitialized runs (see below)
        QueryPrimitives.OrWithMatch(_universe, ref _result.BitmapState, token: _token, preserveLeaf: true);

        // R := R \ M_c for each negated clause, each scoped to the current R via its filter query.
        foreach (var factory in _negatedFactories)
        {
            var clause = factory(_result); // filter query = R (borrowed via LoadFilterMatches, no copy)
            _builtClauses.Add(clause);
            QueryPrimitives.AndNotWithMatch(clause, ref _result.BitmapState, ref _temp, _token);
        }

        _result.BitmapState.PrepareForReading();
    }

    public int Fill(Span<long> matches)
    {
        EnsureInitialized();
        return _result.Fill(matches);
    }

    // A negated post-filter carries no similarity score.
    public void Score(Span<long> matches, Span<float> scores, float boostFactor)
    {
    }

    public void ScoreSorted(Span<long> matches, Span<float> scores, float boostFactor)
    {
    }

    // Side-effect free: must not call EnsureInitialized, since Inspect() can run without the match ever executing.
    public QueryInspectionNode Inspect()
    {
        var parameters = new Dictionary<string, string> { ["IsNegated"] = "true" };

        if (_initialized == false)
        {
            return new QueryInspectionNode(nameof(NegatedPostFilterMatch), parameters: parameters,
                children: [_universe.Inspect()]);
        }

        parameters[Constants.QueryInspectionNode.MatchedResults] = _result.Count.ToString("N0");

        var children = new List<QueryInspectionNode>(_builtClauses.Count + 1) { _universe.Inspect() };
        foreach (var clause in _builtClauses)
            children.Add(clause.Inspect());

        return new QueryInspectionNode(nameof(NegatedPostFilterMatch), parameters: parameters, children: children);
    }

    public void Dispose()
    {
        if (_disposed)
            return;
        _disposed = true;

        foreach (var clause in _builtClauses)
            (clause as IDisposable)?.Dispose();
        (_universe as IDisposable)?.Dispose();

        if (_initialized == false)
            return;
        _result.Dispose();
        _temp.Dispose();
    }
}
