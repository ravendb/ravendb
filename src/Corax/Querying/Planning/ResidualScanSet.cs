namespace Corax.Querying.Planning;

public sealed class ResidualScanSet
{
    public ScanPredicateInfo[] Predicates { get; init; }

    /// <summary> Index of the relevant clause for each predicate (for parameters extraction) </summary>
    public int[] ClauseIndices { get; init; }

    public ResidualScanIlEmitter.ResidualScanPredicate Compiled { get; set; }

    public bool HasPredicates => Predicates is { Length: > 0 };
}
