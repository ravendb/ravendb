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
                (long l, long l2) => IndexSearcher.BetweenQuery(leftmostClause.Name, l, l2,
                    default(NullScoreFunction),
                    leftSide: leftmostClause.BetweenLeft, rightSide: leftmostClause.BetweenRight, fieldId: leftmostClause.FieldId),
                (double d, double d2) => IndexSearcher.BetweenQuery(leftmostClause.Name, d, d2,
                    default(NullScoreFunction),
                    leftSide: leftmostClause.BetweenLeft, rightSide: leftmostClause.BetweenRight, fieldId: leftmostClause.FieldId),
                (string s, string s2) => IndexSearcher.BetweenQuery(leftmostClause.Name, s, s2,
                    default(NullScoreFunction),
                    leftSide: leftmostClause.BetweenLeft, rightSide: leftmostClause.BetweenRight, fieldId: leftmostClause.FieldId),
                (long l, double d) => IndexSearcher.BetweenQuery(leftmostClause.Name, Convert.ToDouble(l), d,
                    default(NullScoreFunction), leftSide: leftmostClause.BetweenLeft, rightSide: leftmostClause.BetweenRight, fieldId: leftmostClause.FieldId),
                (double d, long l) => IndexSearcher.BetweenQuery(leftmostClause.Name, d, Convert.ToDouble(l),
                    default(NullScoreFunction), leftSide: leftmostClause.BetweenLeft, rightSide: leftmostClause.BetweenRight, fieldId: leftmostClause.FieldId),
                _ => throw new InvalidOperationException($"UnaryMatchOperation {leftmostClause.Operation} is not supported for type {leftmostClause.Term.GetType()}")
            };
        }

        return (leftmostClause.Operation, leftmostClause.Term) switch
        {
            (UnaryMatchOperation.LessThan, long l) => IndexSearcher.LessThanQuery(leftmostClause.Name, l, default(NullScoreFunction),
                fieldId: leftmostClause.FieldId),
            (UnaryMatchOperation.LessThan, double d) => IndexSearcher.LessThanQuery(leftmostClause.Name, d, default(NullScoreFunction),
                fieldId: leftmostClause.FieldId),
            (UnaryMatchOperation.LessThan, string s) => IndexSearcher.LessThanQuery(leftmostClause.Name, s, default(NullScoreFunction),
                fieldId: leftmostClause.FieldId),

            (UnaryMatchOperation.LessThanOrEqual, long l) => IndexSearcher.LessThanOrEqualsQuery(leftmostClause.Name, l, default(NullScoreFunction),
                fieldId: leftmostClause.FieldId),
            (UnaryMatchOperation.LessThanOrEqual, double d) => IndexSearcher.LessThanOrEqualsQuery(leftmostClause.Name, d, default(NullScoreFunction),
                fieldId: leftmostClause.FieldId),
            (UnaryMatchOperation.LessThanOrEqual, string s) => IndexSearcher.LessThanOrEqualsQuery(leftmostClause.Name, s, default(NullScoreFunction),
                fieldId: leftmostClause.FieldId),

            (UnaryMatchOperation.GreaterThan, long l) => IndexSearcher.GreaterThanQuery(leftmostClause.Name, l, default(NullScoreFunction),
                fieldId: leftmostClause.FieldId),
            (UnaryMatchOperation.GreaterThan, double d) => IndexSearcher.GreaterThanQuery(leftmostClause.Name, d, default(NullScoreFunction),
                fieldId: leftmostClause.FieldId),
            (UnaryMatchOperation.GreaterThan, string s) => IndexSearcher.GreaterThanQuery(leftmostClause.Name, s, default(NullScoreFunction),
                fieldId: leftmostClause.FieldId),

            (UnaryMatchOperation.GreaterThanOrEqual, long l) => IndexSearcher.GreatThanOrEqualsQuery(leftmostClause.Name, l, default(NullScoreFunction),
                fieldId: leftmostClause.FieldId),
            (UnaryMatchOperation.GreaterThanOrEqual, double d) => IndexSearcher.GreatThanOrEqualsQuery(leftmostClause.Name, d, default(NullScoreFunction),
                fieldId: leftmostClause.FieldId),
            (UnaryMatchOperation.GreaterThanOrEqual, string s) => IndexSearcher.GreatThanOrEqualsQuery(leftmostClause.Name, s, default(NullScoreFunction),
                fieldId: leftmostClause.FieldId),
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
