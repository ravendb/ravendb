namespace Raven.Client.Documents.Indexes;

/// <summary>
/// Index-level default for null placement when ordering (direction-independent).
/// For per-query overrides via <c>NULLS FIRST</c> / <c>NULLS LAST</c>, use
/// <see cref="Raven.Client.Documents.Session.NullsOrdering"/>.
/// </summary>
public enum NullsSortMode
{
    /// <summary>Nulls treated as smallest: first when ascending, last when descending.</summary>
    NullsSmallest,

    /// <summary>Nulls treated as largest: last when ascending, first when descending.</summary>
    NullsLargest
}
