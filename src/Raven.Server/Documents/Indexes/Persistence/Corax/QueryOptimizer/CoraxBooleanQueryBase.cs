using System;
using Corax;
using Corax.Queries;
using Sparrow.Extensions;

namespace Raven.Server.Documents.Indexes.Persistence.Corax.QueryOptimizer;

public abstract class CoraxBooleanQueryBase : IQueryMatch
{
    public abstract IQueryMatch Materialize();
    public readonly IQueryScoreFunction ScoreFunction;
    protected readonly IndexSearcher IndexSearcher;
    protected bool _hasBinary;
    public bool HasBinary => _hasBinary;

    protected CoraxBooleanQueryBase(IndexSearcher indexSearcher, IQueryScoreFunction scoreFunction)
    {
        ScoreFunction = scoreFunction;
        IndexSearcher = indexSearcher;
    }

    protected IQueryMatch TransformCoraxBooleanItemIntoQueryMatch(CoraxBooleanItem leftmostClause)
    {
        if (leftmostClause.Operation is UnaryMatchOperation.Between)
        {
            return (leftmostClause.Term, leftmostClause.Term2) switch
            {
                (long l, long l2) => IndexSearcher.BetweenQuery(leftmostClause.Field, l, l2, default(NullScoreFunction), leftSide: leftmostClause.BetweenLeft, rightSide: leftmostClause.BetweenRight),
                (double d, double d2) => IndexSearcher.BetweenQuery(leftmostClause.Field, d, d2, default(NullScoreFunction), leftSide: leftmostClause.BetweenLeft, rightSide: leftmostClause.BetweenRight),
                (string s, string s2) => IndexSearcher.BetweenQuery(leftmostClause.Field, s, s2, default(NullScoreFunction), leftSide: leftmostClause.BetweenLeft, rightSide: leftmostClause.BetweenRight),
                (long l, double d) => IndexSearcher.BetweenQuery(leftmostClause.Field, Convert.ToDouble(l), d, default(NullScoreFunction), leftSide: leftmostClause.BetweenLeft, rightSide: leftmostClause.BetweenRight),
                (double d, long l) => IndexSearcher.BetweenQuery(leftmostClause.Field, d, Convert.ToDouble(l), default(NullScoreFunction), leftSide: leftmostClause.BetweenLeft, rightSide: leftmostClause.BetweenRight), _ => throw new InvalidOperationException($"UnaryMatchOperation {leftmostClause.Operation} is not supported for type {leftmostClause.Term.GetType()}")
            };
        }

        return (leftmostClause.Operation, leftmostClause.Term) switch
        {
            (UnaryMatchOperation.LessThan, long l) => IndexSearcher.LessThanQuery(leftmostClause.Field, l, default(NullScoreFunction)),
            (UnaryMatchOperation.LessThan, double d) => IndexSearcher.LessThanQuery(leftmostClause.Field, d, default(NullScoreFunction)),
            (UnaryMatchOperation.LessThan, string s) => IndexSearcher.LessThanQuery(leftmostClause.Field, s, default(NullScoreFunction)),

            (UnaryMatchOperation.LessThanOrEqual, long l) => IndexSearcher.LessThanOrEqualsQuery(leftmostClause.Field, l, default(NullScoreFunction)),
            (UnaryMatchOperation.LessThanOrEqual, double d) => IndexSearcher.LessThanOrEqualsQuery(leftmostClause.Field, d, default(NullScoreFunction)),
            (UnaryMatchOperation.LessThanOrEqual, string s) => IndexSearcher.LessThanOrEqualsQuery(leftmostClause.Field, s, default(NullScoreFunction)),

            (UnaryMatchOperation.GreaterThan, long l) => IndexSearcher.GreaterThanQuery(leftmostClause.Field, l, default(NullScoreFunction)),
            (UnaryMatchOperation.GreaterThan, double d) => IndexSearcher.GreaterThanQuery(leftmostClause.Field, d, default(NullScoreFunction)),
            (UnaryMatchOperation.GreaterThan, string s) => IndexSearcher.GreaterThanQuery(leftmostClause.Field, s, default(NullScoreFunction)),

            (UnaryMatchOperation.GreaterThanOrEqual, long l) => IndexSearcher.GreatThanOrEqualsQuery(leftmostClause.Field, l, default(NullScoreFunction)),
            (UnaryMatchOperation.GreaterThanOrEqual, double d) => IndexSearcher.GreatThanOrEqualsQuery(leftmostClause.Field, d, default(NullScoreFunction)),
            (UnaryMatchOperation.GreaterThanOrEqual, string s) => IndexSearcher.GreatThanOrEqualsQuery(leftmostClause.Field, s, default(NullScoreFunction)),
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
    public void Score(Span<long> matches, Span<float> scores) => throw new InvalidOperationException(QueryMatchUsageException);
    public QueryInspectionNode Inspect() => throw new InvalidOperationException(QueryMatchUsageException);

    public bool EqualsScoreFunctions(CoraxBooleanQueryBase other)
    {
        return (ScoreFunction, other.ScoreFunction) switch
        {
            (NullScoreFunction, NullScoreFunction) => true,
            (ConstantScoreFunction c, ConstantScoreFunction d) => c.Value.AlmostEquals(d.Value),
            _ => false
        };
    }
}
