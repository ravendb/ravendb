using System.Runtime.Intrinsics;

namespace Corax.Querying.Planning;

public enum ExecutionStrategy : byte
{
    NotEvaluated = 0,
    // Compute the result set into a bitmap, then sort if needed:
    // from index 'Items' where Name = 'Alice' and Category = 'red' [order by Price]
    BitmapPipeline,
    // Use a compound field index, skip ORDER BY, and merge two field WHERE into a single lookup:
    // from index 'Items' where Category = 'Action' and Age = 40 - with compound(Category,Age)
    CompoundKeyLookup,
    // Scan a compound field index, match the first field equality and then scan in sorted manner, to skip the ORDER BY:
    // from index 'Items' where Category = 'Action' order by Age as long limit 25 - with compound(Category, Age)
    CompoundSortedScan,
    // Stream results from a sorted field directly, without materializing the full result set:
    // from index 'Items' order by Age as long limit 25
    // can also use a range filter + order by: 
    // from index 'Items' where Age > 10 order by Age as long limit 25
    FieldSortedScan,
}

public sealed class CompiledPlan
{
    public PlanTemplate Template { get; init; }

    public QueryIlEmitter.CompiledExecuteDelegate CompiledDelegate { get; init; }
    
    /// <summary>C# source string mirroring emitted IL.</summary>
    public string Source { get; init; }

    public string FormattedSource => field ??= CSharpFormatter.Format(Source);

    /// <summary>
    ///  A single query may be represented by different compiled plans because the shape
    ///  of the data is different. Consider `WHERE Tag = $tag and Published = $published`.
    ///  If $tag is a popular term, and $published is true, the best plan is:
    ///
    ///     Fill(Tag, $tag, bitmap: 0) 
    ///     AndWith(Published, $published, bitmap: 0)
    /// 
    /// On the other hand, if we want all the _unpublished_ items in a popular tag, we can use:
    ///
    ///     Fill(Published, $published, bitmap: 0)
    ///     if (ShouldScan(bitmap))
    ///         EntryScan(bitmap)
    ///     else 
    ///         AndWith(Tag, $tag, bitmap: 0)
    /// 
    ///  In other words, the parameters we use for the query impact the query plan. The digest folds
    ///  every disambiguating dimension (operand ordering, per-parameter runtime type, BETWEEN
    ///  sentinel marks, WHEN-clause survival, boost/cardinality-cliff flags) into one 256-bit value
    ///  used as the cache key — see <see cref="PlanCacheKeyBuilder"/> for the serialization.
    /// </summary>
    public Vector256<long> CacheKeyHash { get; init; }

    public volatile ExecutionStrategy Strategy;

    public PlanDecisionTrail DecisionTrail;

    // The structure we'll use for inspecting the query result when using `include timings()`
    public InspectionOp[] InspectionTemplate { get; init; }

    public int OpCount { get; init; }

    public int RequiredBitmaps { get; init; }

    // when we have small enough entries it is more efficient to scan then continue the bitmap
    public ResidualScanSet EntryScanSet { get; init; }

    // when we iterate over compound field with additional filters
    // e.g. from index 'Items' where Category = 'Action' and Name = 'Alice' order by Age as long limit 25
    //   → CompoundSortedScan walks (Category, Age) tree; Name = 'Alice' is the residual filter
    public ResidualScanSet CompoundFieldResidualSet { get; init; }

    // when we scan a field with additional filters
    // e.g. from index 'Items' where Age > 10 and Name = 'Alice' order by Age as long limit 25
    //   → FieldSortedScan walks Age tree in order; Name = 'Alice' is the residual filter
    public ResidualScanSet DirectScanResidualSet { get; init; }

    // WHEN elimination can remove all non-negated clauses, leaving only negated ones, this tells us if all of there were already negated or not
    public bool AllNegated { get; init; }

    // shared among all executions, feed statistics of the scanned entries for IndexOrderStreaming strategy.
    private InflationEwma _streamScanInflation;

    public InflationEwma GetOrCreateStreamScanInflation()
    {
        // may be racy, that is fine
        return _streamScanInflation ??= new InflationEwma();
    }
}
