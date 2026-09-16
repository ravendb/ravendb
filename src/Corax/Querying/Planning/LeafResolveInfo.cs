using Corax.Mappings;

namespace Corax.Querying.Planning;

public enum LeafResolveKind : byte
{
    /// <summary>Slot is served from <see cref="CompiledQueryMatch.ResolvedMatches"/>.</summary>
    PreResolved,

    /// <summary>Native posting list for a concrete term — resolve via Packed.GetTermPostingListId and PostingSource.Decode.</summary>
    TermPosting,

    /// <summary>Null-term posting list — resolve via IndexSearcher.TryGetPostingListForNull</summary>
    NullPosting,

    /// <summary>Match everything, no-op for AND / OR</summary>
    AllPosting,

    /// <summary>Nothing in this leaf clause OR-shaped ops no-op, AND-shaped ops clear.</summary>
    EmptyPosting,

    /// <summary>CompactTree scan — resolve an <see cref="ITermsProvider"/> (StartsWith / EndsWith / Exists / Regex / range / non-sentinel Between).</summary>
    TreeScan,
}

/// <summary>
/// Provide the query resolution with information on how to resolve this leaf clause inputs
/// </summary>
public struct LeafResolveInfo
{
    // When set to a value (only for ranges), will get observations from query executions to self-correct 
    // over time the estimation based on actual data from query results
    public InflationEwma RangeCalibration;
    // The original estimation for a range clause 
    public long RangeEstimate;
    
    public PackedParam Packed;
    public FieldMetadata FieldMeta;
    public LeafResolveKind Kind;
    public ClauseType ClauseType;
    
}
