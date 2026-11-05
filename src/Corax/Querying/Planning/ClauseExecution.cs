using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;

namespace Corax.Querying.Planning;

/// <summary>Per-execution state for a clause. Populated by PopulateClauseValues each execution (not cached).</summary>
public sealed class ClauseExecution : IComparable<ClauseExecution>
{
    public readonly ClauseInfo Clause;

    public PackedParam PackedParamValue = PackedParam.None;
    public ParamValueType TermValueType;
    public long Cardinality = -1;

    /// <summary>Raw inputs behind this execution's range/StartsWith cardinality estimate, captured for introspection.</summary>
    public RangeEstimateBreakdown? RangeEstimate;

    public int InTermCount;
    public bool HasNullTerm;
    public float BoostFactor;
    public SpatialParams Spatial;
    public VectorParams Vector;

    public ClauseType? SentinelRewriteType;

    public ClauseType ClauseType
    {
        get;
        private set
        {
            if (value is ClauseType.NotEquals)
                IsNegated = true;
            field = value;
        }
    }
    
    public bool IsNegated;

    public List<ClauseExecution> SubExecutions;

    public ClauseExecution(ClauseInfo clause)
    {
        Clause = clause;
        IsNegated = clause.IsNegated;
        ClauseType = clause.ClauseType;
    }

    // A clause is a sentinal if it is a false WHEN() clause, or known upfront to be impossible (like contradictory BETWEEN) 
    public bool IsSentinel => ClauseType is ClauseType.MatchAll or ClauseType.MatchNothing;

    public void MarkAsSentinel(ClauseType sentinel, long cardinality)
    {
        ClauseType = sentinel;
        IsNegated = false; //  the sentinel already subsumes it 
        Cardinality = cardinality;
    }

    // Unlike a false WHEN(), which removes the clause along with its negation, this one *resolved* the operand -
    // so the negation still has to apply, and we fold it into the sentinel: not(nothing) is everything.
    public void MarkAsResolvedSentinel(ClauseType sentinel, long numberOfEntries)
    {
        if (IsNegated)
            sentinel = sentinel is ClauseType.MatchNothing ? ClauseType.MatchAll : ClauseType.MatchNothing;

        MarkAsSentinel(sentinel, sentinel is ClauseType.MatchAll ? numberOfEntries : 0);
    }

    /// <summary>Negated clauses sort last; ties broken by ascending cardinality.</summary>
    public int CompareTo(ClauseExecution other)
    {
        if (IsNegated != other.IsNegated)
            return IsNegated ? 1 : -1;
        return Cardinality.CompareTo(other.Cardinality);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public long GetEffectiveCardinality(IndexSearcher indexSearcher) => Cardinality > 0 ? Cardinality : indexSearcher.NumberOfEntries;
}
