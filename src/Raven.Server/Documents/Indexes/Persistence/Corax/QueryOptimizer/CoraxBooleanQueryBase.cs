using System;
using Corax;
using Corax.Queries;
using Sparrow.Extensions;

namespace Raven.Server.Documents.Indexes.Persistence.Corax.QueryOptimizer;

public abstract class CoraxBooleanQueryBase : IQueryMatch
{
    public abstract IQueryMatch Materialize();
    public float? Boosting;
    protected readonly IndexSearcher IndexSearcher;
    protected bool _hasBinary;
    public bool HasBinary => _hasBinary;

    protected CoraxBooleanQueryBase(IndexSearcher indexSearcher)
    {
        IndexSearcher = indexSearcher;
    }

    protected IQueryMatch TransformCoraxBooleanItemIntoQueryMatch(CoraxBooleanItem leftmostClause)
    {
        if (leftmostClause.Operation is UnaryMatchOperation.Between)
        {
            return (leftmostClause.Term, leftmostClause.Term2) switch
            {
                (long l, long l2) => IndexSearcher.BetweenQuery(leftmostClause.Field, l, l2, leftSide: leftmostClause.BetweenLeft, rightSide: leftmostClause.BetweenRight),
                (double d, double d2) => IndexSearcher.BetweenQuery(leftmostClause.Field, d, d2, leftSide: leftmostClause.BetweenLeft, rightSide: leftmostClause.BetweenRight),
                (string s, string s2) => IndexSearcher.BetweenQuery(leftmostClause.Field, s, s2, leftSide: leftmostClause.BetweenLeft, rightSide: leftmostClause.BetweenRight),
                (long l, double d) => IndexSearcher.BetweenQuery(leftmostClause.Field, Convert.ToDouble(l), d, leftSide: leftmostClause.BetweenLeft, rightSide: leftmostClause.BetweenRight),
                (double d, long l) => IndexSearcher.BetweenQuery(leftmostClause.Field, d, Convert.ToDouble(l), leftSide: leftmostClause.BetweenLeft, rightSide: leftmostClause.BetweenRight), _ => throw new InvalidOperationException($"UnaryMatchOperation {leftmostClause.Operation} is not supported for type {leftmostClause.Term.GetType()}")
            };
        }

        return (leftmostClause.Operation, leftmostClause.Term) switch
        {
            (UnaryMatchOperation.LessThan, long l) => IndexSearcher.LessThanQuery(leftmostClause.Field, l),
            (UnaryMatchOperation.LessThan, double d) => IndexSearcher.LessThanQuery(leftmostClause.Field, d),
            (UnaryMatchOperation.LessThan, string s) => IndexSearcher.LessThanQuery(leftmostClause.Field, s),

            (UnaryMatchOperation.LessThanOrEqual, long l) => IndexSearcher.LessThanOrEqualsQuery(leftmostClause.Field, l),
            (UnaryMatchOperation.LessThanOrEqual, double d) => IndexSearcher.LessThanOrEqualsQuery(leftmostClause.Field, d),
            (UnaryMatchOperation.LessThanOrEqual, string s) => IndexSearcher.LessThanOrEqualsQuery(leftmostClause.Field, s),

            (UnaryMatchOperation.GreaterThan, long l) => IndexSearcher.GreaterThanQuery(leftmostClause.Field, l),
            (UnaryMatchOperation.GreaterThan, double d) => IndexSearcher.GreaterThanQuery(leftmostClause.Field, d),
            (UnaryMatchOperation.GreaterThan, string s) => IndexSearcher.GreaterThanQuery(leftmostClause.Field, s),

            (UnaryMatchOperation.GreaterThanOrEqual, long l) => IndexSearcher.GreatThanOrEqualsQuery(leftmostClause.Field, l),
            (UnaryMatchOperation.GreaterThanOrEqual, double d) => IndexSearcher.GreatThanOrEqualsQuery(leftmostClause.Field, d),
            (UnaryMatchOperation.GreaterThanOrEqual, string s) => IndexSearcher.GreatThanOrEqualsQuery(leftmostClause.Field, s),
            _ => throw new InvalidOperationException($"UnaryMatchOperation {leftmostClause.Operation} is not supported for type {leftmostClause.Term.GetType()}")
        };
    }

    public bool IsBoosting => false;

    protected const string QueryMatchUsageException =
        $"You tried to use {nameof(CoraxBooleanQueryBase)} as normal querying function. This class is only for type - relaxation inside {nameof(CoraxQueryBuilder)} to build big UnaryMatch stack";

    public long Count => throw new InvalidOperationException(QueryMatchUsageException);
    public QueryCountConfidence Confidence => throw new InvalidOperationException(QueryMatchUsageException);
    public int Fill(Span<long> matches) => throw new InvalidOperationException(QueryMatchUsageException);
    public int AndWith(Span<long> buffer, int matches) => throw new InvalidOperationException(QueryMatchUsageException);
    public void Score(Span<long> matches, Span<float> scores, float boostFactor) => throw new InvalidOperationException(QueryMatchUsageException);
    public QueryInspectionNode Inspect() => throw new InvalidOperationException(QueryMatchUsageException);

    protected bool EqualsScoreFunctions(CoraxBooleanQueryBase other)
    {
        if (Boosting is null && other.Boosting is null) return true;
        if (Boosting is null || other.Boosting is null) return false;
        
        return Boosting.Value.AlmostEquals(other.Boosting.Value);
    }
    
    protected bool EqualsScoreFunctions(CoraxBooleanItem other)
    {
        if (Boosting is null && other.Boosting is null) return true;
        if (Boosting is null || other.Boosting is null) return false;
        
        return Boosting.Value.AlmostEquals(other.Boosting.Value);
    }
}
