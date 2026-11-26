using System;

namespace Corax.Querying.Matches.Meta;

public class EmptyQueryMatch : IQueryMatch
{
    public static readonly EmptyQueryMatch Instance = new();
    
    public long Count => 0;
    public bool IsBoosting  => false;
    public int Fill(Span<long> matches) => 0;

    public void Score(Span<long> matches, Span<float> scores, float boostFactor)
    {
    }

    public void ScoreSorted(Span<long> matches, Span<float> scores, float boostFactor)
    {
    }

    public QueryInspectionNode Inspect() => new("Empty");
}
