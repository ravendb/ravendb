namespace Corax.Querying.Planning;

/// <summary>Spatial predicate applied after the bitmap filter phase. ANDs the spatial
/// match result with the candidate bitmap to remove non-matching entries.
/// MatchIndex is the slot in the resolved IQueryMatch[] for this post-filter.</summary>
public struct SpatialFilterOp
{
    public int MatchIndex;
    public ClauseInfo Clause;
    public ClauseExecution Exec;
}